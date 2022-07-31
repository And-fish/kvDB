package lsmt

import (
	"kvdb/file"
	"sync/atomic"
)

type table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // 计数
}

// type tableIterator struct {
// 	item     *utils.Item
// 	opt      *utils.Options
// 	table    *table
// 	blockPos int
// 	bolckI
// }

// 引用次数自增1，ref为0表示可以被回收
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

//

// // 根据levelManager创建table
// func openTable(lm *levelManager, tableName string, buider *tableBuilder) *table {
// 	sstSize := int(lm.opt.SSTableMaxSz)
// 	if buider != nil {
// 		sstSize = int(buider.done().size)
// 	}
// 	var table *table
// 	fid := utils.FID(tableName)
// 	// 如果传入了builder，说明需要根据builder获取table
// 	if buider != nil {
// 		table, err := buider.flush(lm, tableName)
// 		if err != nil {
// 			utils.Err(err)
// 			return nil
// 		}
// 	} else { // 说明要创建一个新的table
// 		// 创建table
// 		table.lm = lm
// 		table.fid = fid
// 		// 创建sstable
// 		table.sst = file.OpenSSTable(&file.Options{
// 			FileName: tableName,
// 			Dir:      lm.opt.WorkDir,
// 			Flag:     os.O_CREATE | os.O_RDWR,
// 			MaxSz:    sstSize,
// 		})
// 	}
// 	table.IncrRef()
// 	// 初始化sstable(根据的是sstable.file也就是MmapFile)
// 	if err := table.sst.Init(); err != nil {
// 		utils.Err(err)
// 		return nil
// 	}

// 	// 创建迭代器
// }
