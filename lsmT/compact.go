package lsmt

import (
	"errors"
	"kvdb/utils"
	"math"
	"sort"
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

/*
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
		thisLevel:    lm.levels[level],
		dropPrefixes: p.dropPrefixes,
	}
}
*/

/*
// 执行一次指定优先级的compact
func (lm *levelManager) run(id int, p compactionPriority) bool {

}
*/

/*
// 执行一次compact
func (lm *levelManager) runOnce(id int) bool {
	// 拿到了每一层的compact优先级
	prios := lm.pickCompactLevels()

	if id == 0 { // id == 0的协程优先压缩
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
*/

/*
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
*/
