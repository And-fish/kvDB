package lsmt

import (
	"sync"
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

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

type compactDef struct {
	compactorID int
	ts          targets
	cp          compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSzie int64

	dropPrefixes [][]byte
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

type thisAndNextLevelRLocked struct{}

/* ============================================================================================ */

// 获取最后一层的levelHandler
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
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

/*
// 选择一个合适的level执行merge，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	ts := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			ts:       ts,
		}
		prios = append(prios, pri)
	}

	// 根据L0的table数量来计算prios
	addPriority(0, float64(lm.levels[0].numTables())/
		float64((lm.opt.NumLevelZeroTables)))

	for i:=1;i<len(lm.levels);i++{
		delSize:=lm.compactState.
	}
}
*/

/* // 执行一次compact
func (lm *levelManager) runOnce(id int) bool {
	lm.pi
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
