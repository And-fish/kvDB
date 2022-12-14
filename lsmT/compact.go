package lsmt

import (
	"errors"
	"fmt"
	"kvdb/pb"
	"kvdb/utils"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// merge的对象
type targets struct {
	baseLevel int     // 目标要merge到的Level
	targetSz  []int64 // 每一层的目标size
	fileSz    []int64 // 每一个file的目标size
}

// compact的优先级
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	ts           targets
}

type thisAndNextLevelRLocked struct{}

/* ============================================================================================ */

// 获取最后一层的levelHandler
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// 获得n个table之间的keyRange
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].sst.GetMinKey()
	maxKey := tables[0].sst.GetMaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].sst.GetMinKey(), minKey) < 0 {
			minKey = tables[i].sst.GetMinKey()
		}
		if utils.CompareKeys(tables[i].sst.GetMaxKey(), maxKey) > 0 {
			maxKey = tables[i].sst.GetMaxKey()
		}
	}

	return keyRange{
		// keyRange不需要设置时间戳，这样设置可以保证keyRange可以覆盖所有的key
		// left == [realKey + Uint64(0)]
		// right == [realKey + MaxUint64]
		left:  utils.KeyWithTS(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTS(utils.ParseKey(maxKey), 0),
	}
}

// 选择一个适合merge的level
func (lm *levelManager) levelTargets() targets {
	adjust := func(size int64) int64 { // 用于判断，返回BaseLevelSize 和 size中更大的那个
		if size < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return size
	}

	// 默认初始化为为最大level
	ts := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}

	// 从最后一个level开始计算
	dbSize := lm.lastLevel().getTotalSize()   // 最后一层的总size
	for i := len(lm.levels) - 1; i > 0; i-- { // 从最后一层开始检查
		targetSize := adjust(dbSize)
		ts.targetSz[i] = targetSize // 每一层的targetSize是每一层totalSzie 和 baseLevelSize的最大值

		// 如果当前level没有达到合并的要求，当这个if被触发后就不会在后续被触发，所以这里能尽可能选择N更大的level
		if ts.baseLevel == 0 && targetSize <= lm.opt.BaseLevelSize { // 没有达到阈值
			ts.baseLevel = i // 将baseLevel设置为 i层
		}
		dbSize = dbSize / int64(lm.opt.LevelSizeMultiplier) // 计算每一层的dbSzie，从下到上每一层都会少一个数量级
	}

	// 如果levelManger是第一次compact： ts.baseLevel == 6；ts.targetSz == [1G,1G,1G,1G,1G,1G,1G]
	// 如果不是第一次compact，且L6 == 20G ：ts.baseLevel == 4；ts.targetSz == [1G,1G,1G,1G,1G,2G,20G]

	// 从0层开始计算每一层的table个数
	tableSize := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// L0层选择memtable的size作为文件的尺寸，因为L0没有经过merge，sst文件就是按照opt创建的
			ts.fileSz[i] = lm.opt.MemTableSize
		} else if i <= ts.baseLevel {
			// L0 ~ Lbase：tablesize == lm.opt.BaseTableSize
			ts.fileSz[i] = tableSize
		} else {
			// 超过Lbase的：tablesize每层翻十倍
			tableSize *= int64(lm.opt.TableSizeMultiplier) // 每一层递增一个数量级
			ts.fileSz[i] = tableSize
		}
	}

	// 找到最后一个空level作为目标level实现跨level compact，减少写放大
	for i := ts.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		ts.baseLevel = i
	}

	// 如果存在断层，则目标level ++
	base := ts.baseLevel
	lhs := lm.levels
	// 如果不是最后一层 && 本层为size为0 && 下一层没有到阈值
	if base < len(lhs)-1 && lhs[base].getTotalSize() == 0 && lhs[base+1].getTotalSize() < ts.targetSz[base+1] {
		ts.baseLevel++
	}
	// 假设经过多个compact后 L6 == 20G ，经过了计算后 ：ts.baseLevel == ；ts.targetSz == [1G,1G,1G,1G,1G,2G,20G]；
	// 此时 lhs[4].getTotalSize() == 0，lhs[4].getTotalSize() < ts.targetSz[5] == 2G
	// 说明L5还能装，所以将base设置为5

	return ts
}

// 选择一个合适的level执行merge，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	ts := lm.levelTargets()
	addPriority := func(level int, score float64) { // 将每一层的pri添加到 []compactionPriority中
		pri := compactionPriority{ // 生成compactionPriority
			level:    level,
			score:    score, // 判断当前是否达到了targetSize
			adjusted: score, // 用作优先级
			ts:       ts,
		}
		prios = append(prios, pri)
	}

	// 根据L0的table数量来计算prios，并将L0的优先级添加到[]compactionPriority中
	addPriority(0, float64(lm.levels[0].numTables())/
		float64((lm.opt.NumLevelZeroTables))) // score = L0的table数量 / Option中最大的L0数量 ；因为L0的tableSize是固定的，不需要计算Size

	for i := 1; i < len(lm.levels); i++ {
		delSize := lm.compactState.delSize(i) // 获取每一层的delSize，delSize就是当前正在参与compact的Table的Size
		level := lm.levels[i]
		size := level.getTotalSize() - delSize                // 剩下的Size就是本次compact需要考虑的size
		addPriority(i, float64(size)/float64(ts.targetSz[i])) // score = 剩下的size / 目标size；Ln的table数量没有参考价值
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// 修正adjusted
	//
	// 如果score过小，说明当前层很空，如果socre >=1，说明当前层满了
	var prevLevel int
	for level := ts.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 { // 如果调整前preLevel的值不小于1就需要修正得分，也就是说，只会修正满了的level的分数
			const minScore = 0.01 // 一个修正参数
			// 将上一层的分数放大 / 缩小
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted // 如果本层没有很满，就优先compact preLevel，如果本层满了，就降低上一层的优先级
			} else { // 如果本层很空，放大perLevel的优先级，但是控制在放大100倍
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 选择source大于1的作为compact的对象，也就是满了的level(超过了targetSize)
	out := prios[:0] // 一种优化性能的方法，会和prios共用同一个底层数组和cap
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按照优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

// 按照将L0层放到优先级数组的最前面，使得可以优先compact L0
func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}

	// 如果idx == -1，说明prios中没有L0，说明L0没有到达targetSize(L0层的table数量小于 option中的L0数量)
	// 如果idx == 0，说明L0就在prios的第0个，不需要做任何操作
	// 如果idx > 0，会将L0所在的compactionPriority放到最前面
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// 将keyRange切分为多个小的keyRange，方便并行执行compact
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// 每个携程执行一个
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	kr := cd.thisRange
	kr.extend(cd.nextRange)

	// 用于切分
	addRange := func(right []byte) {
		kr.right = utils.Copy(right)
		cd.splits = append(cd.splits, kr)
		kr.left = kr.right
	}

	for i, t := range cd.bot {
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			right := utils.KeyWithTS(utils.ParseKey(t.sst.GetMaxKey()), math.MaxUint32)
			addRange(right)
		}
	}
}

// 从最后一个table开始，为每一个table都创建一个tableIterator
func iteratorsReversed(ts []*table, opt *utils.Options) []utils.Iterator {
	iters := make([]utils.Iterator, 0, len(ts))
	for i := len(ts) - 1; i >= 0; i-- {
		iters = append(iters, ts[i].NewIterator(opt)) // 向里面添加一个tableIterator
	}
	return iters
}

// 将需要丢失的数据传出
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// 判断是否过期
func IsDeletedOrExpired(entry *utils.Entry) bool {
	if entry.Value == nil {
		return true
	}
	if entry.TTL == 0 {
		return false
	}
	return entry.TTL <= uint64(time.Now().Unix())
}

// 开始执行并行compact
func (lm *levelManager) subcompact(itr utils.Iterator, kr keyRange, cd compactDef, va *utils.Valve, res chan<- *table) {
	var lastKey []byte
	discardStats := make(map[uint32]int64) // 收集过期数据，fid / size
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()

	updateStats := func(entry *utils.Entry) {
		if entry.Meta&utils.BitValuePointer > 0 { // 如果被标记为ValuePtr，会添加到discardStats
			var vp utils.ValuePtr
			vp.Decode(entry.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}

	addKeys := func(builder *tableBuilder) {
		var tkr keyRange
		for ; itr.Valid(); itr.Next() {
			key := itr.Item().Entry().Key
			isExpied := IsDeletedOrExpired(itr.Item().Entry())
			if !utils.IsSameKey(key, lastKey) {
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.IsReachedCapacity() {
					break
				}
				lastKey = utils.SafeCopy(lastKey, key)
				if len(tkr.left) == 0 {
					tkr.left = utils.SafeCopy(tkr.left, key)
				}
				tkr.right = lastKey
			}

			switch {
			case isExpied: // 如果已经过期了会执行check是否是ValuePtr
				updateStats(itr.Item().Entry())
				builder.AddStaleKey(itr.Item().Entry()) // 将过期数据记录下来
			default:
				builder.Addkey(itr.Item().Entry()) // 没有过期就Add
			}
		}
	}

	if len(kr.left) > 0 {
		itr.Seek(kr.left)
	} else {
		itr.Rewind()
	}
	for itr.Valid() {
		key := itr.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		builder := newTableBuilderWithSize(lm.opt, cd.ts.fileSz[cd.nextLevel.levelNum])

		addKeys(builder)
		if builder.isEmpty() {
			builder.finish()
			builder.Close()
			continue
		}
		if err := va.Run(); err != nil {
			break
		}

		go func(builder *tableBuilder) {
			defer va.Done(nil)
			defer builder.Close()

			var t *table
			fid := atomic.AddUint64(&lm.maxFID, 1)
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)
			t = openTable(lm, sstName, builder) // 这里会增加一次引用
			if t == nil {
				return
			}
			res <- t
		}(builder)
	}
}

// 根据compact计划创建builder
func (lm *levelManager) compactBuildTables(i int, cd compactDef) ([]*table, func() error, error) {
	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}

	// 创建一组迭代器
	newIter := func() []utils.Iterator {
		var iters []utils.Iterator
		switch {
		case i == 0:
			// 传入的opt.IsAsc == true，所以创建的的[]iterator，Rewind后会跳转到最后一个tableInterator(next会跳转到上一个TableIntera的第一个block的第一个entey)
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// 开始并行的执行压缩过程
	res := make(chan *table, 3)
	valve := utils.NewValue(8 + len(cd.splits))
	for _, kr := range cd.splits {
		if err := valve.Run(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}

		go func(kr keyRange) {
			defer valve.Done(nil)

			it := NewMergeIterator(newIter(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, valve, res) // 会调用openTables增加一次对table的引用
		}(kr)
	}

	var tables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			tables = append(tables, t)
		}
	}()

	err := valve.Finish()
	close(res)
	wg.Wait()

	if err == nil {
		err = utils.SyncDir(lm.opt.WorkDir)
	}
	if err != nil {
		decrRefs(tables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(tables, func(i, j int) bool {
		return utils.CompareKeys(tables[i].sst.GetMaxKey(), tables[j].sst.GetMaxKey()) < 0
	})
	return tables, func() error {
		return decrRefs(tables)
	}, nil
}

// 创建Create的ManifestChange
func newCreateChange(tableID uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    tableID,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// 创建Delete的ManifestChange
func newDeleteChange(tableID uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: tableID,
		Op: pb.ManifestChange_DELETE,
		// DELETE 不需要level
	}
}

// 根据compactDef创建ManifestchangeSet
func buildChangeSet(cd *compactDef, tables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range tables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{
		Changes: changes,
	}
}

// 总的执行压缩计划入口
func (lm *levelManager) runCompactDef(id, i int, cd compactDef) (err error) {
	if len(cd.ts.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	now := time.Now()
	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))

	if thisLevel == nextLevel {
		// L0 --> L0 || Lmax --> Lmax
		// 不做特殊处理
	} else {
		lm.addSplits(&cd)
	}
	//
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	tables, decrFunc, err := lm.compactBuildTables(i, cd) //这里会增加一次会tables的引用
	if err != nil {
		return err
	}
	defer func() {
		// 减少一次对tables引用
		if decErr := decrFunc(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, tables)

	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}
	// 在levelHandler中删除旧的table cd.bot，添加新的tables
	if err := nextLevel.replaceTables(cd.bot, tables); err != nil { // 这里会增加一次对tables引用
		return err
	}
	// 还会删除cd.top
	defer decrRefs(cd.top)
	// 删除减少一次引用，并将top中thislevel中删除
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// DEBUG
	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(tables)
	if dur := time.Since(now); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(tables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// 选择当前level的某些tabel合并到目标table
func (lm *levelManager) doComapct(id int, p compactionPriority) error {
	level := p.level
	utils.CondPanic(level >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	// 如果目标targetLevel是L0，就在重新选择一个TargetLevel
	if p.ts.baseLevel == 0 {
		p.ts = lm.levelTargets()
	}

	// 创建真正的压缩计划
	cd := compactDef{
		compactorID:  id,
		cp:           p,
		ts:           p.ts,
		thisLevel:    lm.levels[level], //
		dropPrefixes: p.dropPrefixes,
	}

	// 如果是第0层，会单独处理
	if level == 0 {
		cd.nextLevel = lm.levels[p.ts.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		if !cd.nextLevel.isLastLevel() { // 如果当前不是在最后一层，compact到下一层
			cd.nextLevel = lm.levels[level+1]
		}
		if !lm.fillTables(&cd) { // compact
			return utils.ErrFillTables
		}
	}

	// compactDef success
	defer lm.compactState.delete(&cd)

	// 执行策略
	if err := lm.runCompactDef(id, level, cd); err != nil {
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// 执行一次指定优先级的compact
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doComapct(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// 执行一次compact
func (lm *levelManager) runOnce(id int) bool {
	// 拿到了每一层的compact优先级
	prios := lm.pickCompactLevels()

	if id == 0 { // id == 0的协程优先压缩L0
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 { // goRoutine == 0在compact L0时，不论如何都会执行
		} else if p.adjusted < 1.0 { // 对于其他的Level，如果优先分数小于 1 ，不会compact
			break
		}
		if lm.run(id, p) { // 执行compact
			return true
		}
	}
	// 优先分数小于 1的level会跳出到这里，返回没有compact
	return false
}

// 启动一个定时compact程序(协程)
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	// 随机一个时间开始，避免同时启动过多的程序
	randTimer := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond) // 1000毫秒
	select {
	case <-randTimer.C:
	case <-lm.lsm.closer.CloseSignal:
		randTimer.Stop()
		return
	}

	// 启动一个定时器，定时执行compact
	ticker := time.NewTicker(50000 * time.Millisecond) // 50秒
	defer ticker.Stop()
	for {
		select {
		// 定时检查compact
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

// debug，tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}
