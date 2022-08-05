package lsmt

import (
	"bytes"
	"fmt"
	"kvdb/utils"
	"log"
	"sync"
)

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
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

var infRange = keyRange{
	inf: true,
}

// debug
func (kr keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", kr.left, kr.right, kr.inf)
}

// 判断是否为空值
func (kr keyRange) isEmpty() bool {
	return len(kr.left) == 0 && len(kr.right) == 0 && !kr.inf
}

// 判断两个keyRange是否相等
func (kr keyRange) equals(dst keyRange) bool {
	return bytes.Equal(kr.right, dst.right) &&
		bytes.Equal(kr.left, dst.left) &&
		kr.inf == dst.inf
}

// 将kr扩展按照ekr来扩展
func (kr *keyRange) extend(ekr keyRange) {
	if kr.isEmpty() {
		return
	}
	if ekr.isEmpty() {

	}
	// 哪个keyRange的左边界更左就取哪一个
	if len(kr.left) == 0 || utils.CompareKeys(ekr.left, kr.left) < 0 {
		kr.left = ekr.left
	}
	// 哪个keyRange的右边界更右就取哪一个
	if len(kr.right) == 0 || utils.CompareKeys(ekr.right, kr.right) > 0 {
		kr.right = ekr.right
	}
	if ekr.inf {
		kr.inf = true
	}
}

// 判断是否重合
func (kr keyRange) overlapsWith(dst keyRange) bool {
	// 空的keyRange总是和任何keyRange重合
	if kr.isEmpty() {
		return true
	}
	// keyRange不会和任何空的keyRange重合
	if dst.isEmpty() {
		return false
	}
	if kr.inf || dst.inf {
		return true
	}
	// [dst.left , dst.right] ... [kr.left , kr.right]
	// 如果kr的左边界大于dst的右边界，说明不可能重合
	if utils.CompareKeys(kr.left, dst.right) > 0 {
		return false
	}
	// [kr.left , kr.right] ... [dst.left , dst.right]
	// 如果kr的右边界小于dst的左边界，说明不可能重合
	if utils.CompareKeys(kr.right, dst.left) < 0 {
		return false
	}
	return true
}

// 判断本层是否有和dst重合的
func (lcs *levelCompactStatus) overLapsWith(dst keyRange) bool {
	for _, kr := range lcs.ranges {
		if kr.overlapsWith(dst) {
			return true
		}
	}
	return false
}

// 移除本层的某个keyRange，返回本level中是否曾经存在这个keyRange
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, kr := range lcs.ranges {
		if !kr.equals(dst) { // 如果不相等就加到最终的ranges名单中
			final = append(final, kr)
		} else { // 如果遇到了相等的会返回true
			found = true
		}
	}
	lcs.ranges = final
	return found
}
func (lcs *levelCompactStatus) debug() string {
	var buf bytes.Buffer
	for _, kr := range lcs.ranges {
		buf.WriteString(kr.String())
	}
	return buf.String()
}

// 创建新的compactStatus
func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

// 判断某一层中是否和指定keyRange重合的
func (cs *compactStatus) overlapsWith(i int, kr keyRange) bool {
	cs.RLock() // 保证读一致性，要加读锁
	defer cs.RUnlock()

	level := cs.levels[i]
	return level.overLapsWith(kr)
}

// 获取指定level需要删除的size
func (cs *compactStatus) delSize(i int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[i].delSize
}

// 检查是否有冲突，并将compactDef中的计划添加到compactStatus中，并将bot中的tables append到top中
func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	cs.Lock()
	defer cs.Unlock()

	i := cd.thisLevel.levelNum
	utils.CondPanic(i >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", i, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum] // 待compact的层级
	nextLevel := cs.levels[cd.nextLevel.levelNum] // compact到的层级

	// 判断thisLevel中是否有和计划重合的
	if thisLevel.overLapsWith(cd.thisRange) {
		return false
	}
	// 判断nextLevel中是否有和计划重合的
	if nextLevel.overLapsWith(cd.nextRange) {
		return false
	}

	// 如果没有重合，就将计划添加到各层的Ranges里面
	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSzie // 增加已经被选中的
	// 添加tables到 top 和 cs.tables中
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}

	return true
}

// 在compactStatus中删除compactDef中的变更
func (cs *compactStatus) delete(cd *compactDef) {
	cs.Lock()
	defer cs.Unlock()

	level := cd.thisLevel.levelNum
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSzie
	found := thisLevel.remove(cd.thisRange) // 检查并删除thisLevel中是否存在ThiskeyRange

	//如果nextRange为空不需要删除 或者 如果是thisLevel == nextLevel 也不需要删除(只会发生在Lmax --> Lmax的时候，这时候这个会在下一轮到Lmax的compact时被覆盖)
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		//
		found = nextLevel.remove(cd.nextRange) && found
	}

	// DEBUG
	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, level)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	// 在全局变量的compactStatus中删除bot和top的tables
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}
