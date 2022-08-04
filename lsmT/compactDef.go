package lsmt

import (
	"sort"
)

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

/* // 对Lmax 到Lmax 的compact
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
	// 这个函数会聚合table到next中
	collectBotTables := func(table *table, size int64) {
		totalSize := table.GetSize()
		idx := sort.Search(len(tables), func(i int) bool { // 在tables中找到一个刚刚大于等于指定table的table
			return utils.CompareKeys(tables[i].sst.GetMinKey(), table.sst.GetMinKey()) >= 0
		})
		utils.CondPanic(tables[idx].fid != table.fid, errors.New("tables[j].ID() != t.ID()"))
		// 因为tables是按照minKey升序排序，所以后续所有table的minkey都大于给定table的minkey
		// 遍历后续的所有table，并将其全部加入到MergeGroup中
		idx++
		for idx < len(tables) {
			newTable := tables[idx]
			totalSize += newTable.sst.Size()
			if totalSize >= size {
				break
			}
			cd.bot = append(cd.bot, newTable)
			cd.nextRange.extend(getKeyRange(newTable)) // 扩充nextRange
			idx++
		}
	}

	now := time.Now()
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
		cd.thisRange = getKeyRange(t)
		cd.nextRange = cd.thisRange
		// 如果本层中有和这个table重合的就跳过
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// 到这一步说明，该table可能需要被合并
		cd.top = []*table{t}
		needSize := cd.ts.fileSz[cd.thisLevel.levelNum] // 单个sst文件的阈值大小

		if t.GetSize() >= needSize {
			break
		}
		// 如果table的大小 小于规定的大小，就尝试将这个小的table合并为一个大的table
		collectBotTables(t, needSize) // Search到的是本table，合并的都是大于thisTable.MinKey的table
		// if lm.compactState.
	}
} */

// func (lm *levelManager) fillTables(cd *compactDef) bool {
// 	cd.lockLevels()
// 	defer cd.unlockLevels()

// 	tables := make([]*table, cd.thisLevel.numTables())
// 	copy(tables, cd.thisLevel.tables)
// 	if len(tables) == 0 {
// 		return false
// 	}

// 	if cd.thisLevel.isLastLevel() {
// 		return lm.fillMaxLevelTables()
// 	}
// }
