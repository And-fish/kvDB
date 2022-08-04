package lsmt

import (
	"bytes"
	"kvdb/file"
	"kvdb/utils"
	"sort"
	"sync"
	"sync/atomic"
)

type levelManager struct {
	maxFID       uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt          *Options
	cache        *cache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
	lsm          *LSM
	compactState *compactStatus
}

// --------- level处理器 -------
type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

// 添加table
func (lh *levelHandler) add(table *table) {
	lh.Lock()
	defer lh.Unlock()

	lh.tables = append(lh.tables, table)
}

// 批添加tables
func (lh *levelHandler) addBatch(tables []*table) {
	lh.Lock()
	defer lh.Unlock()

	lh.tables = append(lh.tables, tables...)
}

// close
func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].sst.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 返回本level的总size
func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()

	return lh.totalSize
}

// 当本层添加了新的table，也会对totalsize增加table大小
func (lh *levelHandler) addSize(table *table) {
	lh.totalSize += table.GetSize()
	lh.totalStaleSize += int64(table.StaleDataSize())
}

// 返回对应level的table数量
func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.Unlock()

	return len(lh.tables)
}

// 排序
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()

	if lh.levelNum == 0 {
		// 第0层按照fid排序
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// 其他层按照Minkey大小排序(从小到大)
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].sst.GetMinKey(), lh.tables[j].sst.GetMinKey()) < 0
		})
	}
}

// 根据key获取levelHandler中所在的table	(L0层不可用)
func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		// 每个table都会记录各自的minkey和maxkey，如果key在[minKey,maxKey]区间内就会返回
		if bytes.Compare(key, lh.tables[i].sst.GetMinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].sst.GetMaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}

// L0层需要循环遍历查找
func (lh *levelHandler) searchL0SST(key []byte) (*utils.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Search(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// LN层可以使用直接获取对应的table
func (lh *levelHandler) searchLNSST(key []byte) (*utils.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table != nil {
		if entry, err := table.Search(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// 对所有查询做的包装
func (lh *levelHandler) Get(key []byte) (*utils.Entry, error) {
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	} else {
		return lh.searchLNSST(key)
	}
}

// 检查是不是到最后一个level
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum
}

// TODO
// func (lh *levelHandler) iterators() []utils.Iterator {
// 	lh.Lock()
// 	defer lh.RLock()

// 	opt := utils.Options{IsAsc: true}
// 	if lh.levelNum ==0{
// 		return iter
// 	}
// }

// levelManager

// 关闭levelManager
func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil { // 关闭缓存
		return err
	}
	if err := lm.manifestFile.Close(); err != nil { // 关闭manifestFile
		return err
	}
	// 关闭每一层
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil { //每一层中循环遍历关闭所有的table
			return err
		}
	}
	return nil
}

//TODO 迭代器

// 从levelManger中获取查找
func (lm *levelManager) Get(key []byte) (*utils.Entry, error) {
	var entry *utils.Entry
	var err error
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		lh := lm.levels[level]
		if entry, err = lh.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

// TODO
func (lm *levelManager) loadCache() {}

// 加载WorkDir下面的MANIFEST文件，如果不存在就创建一个
func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{
		Dir: lm.opt.WorkDir,
	})
	return err
}

// build构建一个levelMangaer
func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,                 // 对应每个levelHandler在哪一Level
			tables:   make([]*table, 0), // 其中有哪些tables
			lm:       lm,
		})
	}

	// ManifestFile在loadManifest()中构建
	manifest := lm.manifestFile.GetManifest()
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}

	// 加载所有table中的sstable去构建cache
	lm.cache = newCache()
	// tableIndex被加载到了sstable中，这里会减少读磁盘，但是会增大内存的消耗
	var maxFID uint64
	for fid, tableManifest := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fid)
		if fid > maxFID {
			maxFID = fid
		}
		table := openTable(lm, fileName, nil)
		lm.levels[tableManifest.Level].add(table)
		lm.levels[tableManifest.Level].addSize(table)
	}

	// 对每一层进行排序
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// 获得最大的fid
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

// 向L0层flush一个SStable
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// 首先分配一个唯一的FID
	fid := immutable.wal.GetFid()
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	// 构建builder
	builder := newTableBuilder(lm.opt)
	iter := immutable.sl.NewSkiplistIterator()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.add(entry, false)
	}

	// 创建一个table对象
	table := openTable(lm, sstName, builder)
	// 写入到manifest中
	err = lm.manifestFile.AddTableMeta(0, &file.TableMeta{
		ID:       fid,
		Checksum: []byte{'X', 'X', 'I', 'H'},
	})
	utils.Panic(err)
	// 更新第0层的levelHandler
	lm.levels[0].add(table)
	return
}

// 初始化LevelManager
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} // 反引用
	// TODO  lm.compactState =lsm.newcom
	lm.opt = opt

	// 构建manifest
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}
