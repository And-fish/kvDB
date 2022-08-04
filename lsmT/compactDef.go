package lsmt

import (
	"errors"
	"kvdb/utils"
	"math"
	"sort"
	"time"
)

// 压缩计划 compact deflate
type compactDef struct {
	compactorID int
	ts          targets
	cp          compactionPriority
	thisLevel   *levelHandler // 应该要compact哪个level
	nextLevel   *levelHandler // 应该要compact到哪个level

	top []*table // 待compact的table
	bot []*table // 待确认是否compact的table

	thisRange keyRange // 待compact的keyRange
	nextRange keyRange // 目标level1的keyRange
	splits    []keyRange

	thisSzie int64

	dropPrefixes [][]byte
}

// 加锁
func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

// 解锁
func (cd *compactDef) unlockLevels() {
	cd.thisLevel.RUnlock()
	cd.nextLevel.RUnlock()
}

// 按照tables中陈旧数据的数量对tables进行排序(从大到小)
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// 按照 sst文件中陈旧数据的数量排序(从大到小)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// 按照tables中新旧程度对tables进行排序(从小到大)
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// 按照table的maxVersion排序(从小到大)
	// maxVersion是一个单调递增的值，如果version越大说明这个Entry越新，拥有更新的sst也可以被认为更新
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].sst.GetIndexs().MaxVersion < tables[j].sst.GetIndexs().MaxVersion
	})
}

// 对L0 到baseLevel的compact
func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}
	// 如果adjusted小于1说明不需要compact，直接返回
	if cd.cp.adjusted > 0.0 && cd.cp.adjusted < 1.0 {
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// L0层的tables是按照fid升序排序的，所以这里会优先compact旧的
	for _, t := range top {
		tkr := getKeyRange(t)
		// 如果table之间重合就会合并
		if tkr.overlapsWith(tkr) {
			out = append(out, t) // 并将table添加到待compact的数组
			kr.extend(tkr)       // 扩展keyRange的大小
		} else {
			// 如果遇到不重叠的会终止
			break
		}
	}
	// 获取L0层待compact的keyRange
	cd.thisRange = getKeyRange(out...)
	cd.top = out // 确认待compact的sst

	// 在baseTable中找到会与L0层待compact的tables重合的tables
	leftIdx, rightIdx := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, rightIdx-leftIdx)
	copy(cd.bot, cd.nextLevel.tables[leftIdx:rightIdx])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}

	// check一下并add到top中
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// 对L0 到L0的compact
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorID != 0 {
		// 只允许0号协程执行L0到L0的compact，避免竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.GetSize() >= 2*cd.ts.fileSz[0] {
			// 如果table过大，compact到本层会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果创建时间小于10s，直接跳过
			continue
		}
		if _, ok := lm.compactState.tables[t.fid]; ok {
			// 如果这个table正在被compact，直接跳过
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 需要压缩的tables过少会取消压缩
		return false
	}
	cd.thisRange = infRange
	cd.top = out
	// infRange会避免任何L0的compact
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	// L0到L0的压缩最终会被压缩为一个文件，会大大减少读放大
	cd.ts.fileSz[0] = math.MaxUint32
	return true
}

// 对L0的compact
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	// 首先会尝试L0到BaseLevel的compact
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	// 如果失败了，再尝试L0到L0的compact
	return lm.fillTablesL0ToL0(cd)
}

// 对Lmax 到Lmax 的compact
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	// 按照陈旧数据进行排序
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	// 如果没有陈旧数据，返回false
	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		return false
	}
	cd.bot = []*table{}
	// 这个函数会聚合table到bot 和 nextRange中
	collectBotTables := func(table *table, size int64) {
		totalSize := table.GetSize()
		idx := sort.Search(len(tables), func(i int) bool { // 在tables中找到一个刚刚大于等于指定table的table
			return utils.CompareKeys(tables[i].sst.GetMinKey(), table.sst.GetMinKey()) >= 0
		})
		utils.CondPanic(tables[idx].fid != table.fid, errors.New("tables[j].ID() != t.ID()"))
		// 因为tables是按照minKey升序排序，所以后续所有table的minkey都大于给定table的minkey
		// 遍历后续的所有table，并尝试将其全部加入到MergeGroup中
		idx++
		for idx < len(tables) {
			newTable := tables[idx]
			totalSize += newTable.sst.Size()
			if totalSize >= size { // 如果总的Size超过了单个sst的阈值就停止
				break
			}
			cd.bot = append(cd.bot, newTable)          // 添加这个table到待compact的tables中
			cd.nextRange.extend(getKeyRange(newTable)) // 扩充nextRange
			idx++
		}
	}

	now := time.Now()
	// 从陈旧数据多的table开始，遍历所有的table
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// 不会合并一个小时内的table
			continue
		}
		if t.StaleDataSize() < 10<<20 { // 10MB
			// 如果陈旧数据过少，没有必要合并
			continue
		}

		cd.thisSzie = t.GetSize()
		cd.thisRange = getKeyRange(t) // 假设需要compact这个table，所以默认待compact的keyRange是这个table的KeyRange
		cd.nextRange = cd.thisRange   // 获取合并后的keyRange，默认一开始 == thisRange
		// 如果本层的状态表中有和这个table重合的，说明有其他协程正在compact这个table就跳过
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// 到这一步说明，该table可能需要被合并
		cd.top = []*table{t}
		needSize := cd.ts.fileSz[cd.thisLevel.levelNum] // 单个sst文件的阈值大小

		if t.GetSize() >= needSize { // 如果sst已经超过阈值了就取消
			break
		}
		// 如果table的大小 小于规定的大小，就尝试将这个小的table和其他table合并为一个大的table
		collectBotTables(t, needSize) // Search到的是本table，合并的都是大于thisTable.MinKey的table
		// compactDef指定完成，开始检查并将compactDef中
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			// 如果有冲突就放弃这个table的compact，尝试合并陈旧数据稍微小一点的table
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		// compareAndAdd通过就会return
		return true
	}
	if len(cd.top) == 0 {
		return false
	}
	// 最后再compareAndAdd
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// Ln的compact
func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// Lmax --> Lmax会特殊处理
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// 按照新旧来排序，优先compact旧的table
	lm.sortByHeuristic(tables, cd)

	for _, t := range tables { // 遍历所有table，优先compact最旧的table
		cd.thisSzie = t.GetSize()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩了，就放弃本次table
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		// 在nextLevel中查找和keyRange重合的tables
		leftIdx, rightIdx := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
		cd.bot = make([]*table, rightIdx-leftIdx)
		copy(cd.bot, cd.nextLevel.tables[leftIdx:rightIdx])

		// 如果待确认compact的tables为空，也就是在nextLevel中没有找到重合的
		if len(cd.bot) == 0 {
			cd.bot = []*table{} //置为空
			cd.nextRange = cd.thisRange
			// check并添加本层重合的tables到bot中
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				// 如果发生了冲突，放弃本次table
				continue
			}
			// 添加成功返回true
			return true
		}

		// 计算compact后的keyRange
		cd.nextRange = getKeyRange(cd.bot...)

		// 判断nextLevel中是否有和merge后的keyRange发生冲突的
		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		// check并添加本层的tables
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		// 如果添加本层tables没有问题，return true
		return true
	}
	return false
}
