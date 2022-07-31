package lsmt

import (
	"encoding/binary"

	"fmt"
	"kvdb/file"
	"kvdb/pb"
	"kvdb/utils"
	"math"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // 计数
}

// 引用次数自增1，ref为0表示可以被回收
func (table *table) IncrRef() {
	atomic.AddInt32(&table.ref, 1)
}

// 获取table对应的sstable.file文件的大小
func (table *table) GetSize() int64 {
	return table.sst.Size()
}

// GetCreatedAt创建时间
func (table *table) GetCreatedAt() *time.Time {
	return table.sst.GetCreatedAt()
}

// Delete删除对应的sstable.file
func (table *table) Delete() error {
	return table.sst.Delete()
}

// 用于setBlocks的Cache时用到的key
func (table *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(table.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(table.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

// 引用减一，表示不再引用；如果已经不被引用了，就会触发删除流程
func (table *table) DecrRef() error {
	ref := atomic.AddInt32(&table.ref, -1)
	if ref == 0 {
		for i := 0; i < len(table.sst.GetIndexs().GetOffsets()); i++ {
			table.lm.cache.blocks.Del(table.blockCacheKey(i))
		}
		if err := table.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// // 根据levelManager创建table
// func openTable(lm *levelManager, tableName string, buider *tableBuilder) *table {
// 	sstSize := int(lm.opt.SSTableMaxSz)
// 	if buider != nil {
// 		sstSize = int(buider.done().size)
// 	}
// 	var table *table
// 	var err error
// 	fid := utils.FID(tableName)
// 	// 如果传入了builder，说明需要根据builder获取table
// 	if buider != nil {
// 		table, err = buider.flush(lm, tableName)
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
//
// }

// 获取table的对应i的BlockOffset
func (table *table) offsets(BOffset *pb.BlockOffset, i int) bool {
	index := table.sst.GetIndexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*BOffset = *index.GetOffsets()[i]
	return true
}

// read可以对table.sst.file.data从offset开始读取size
func (table *table) read(offset, size int) ([]byte, error) {
	// 对mmapFile的data读取
	return table.sst.Btyes(offset, size)
}

// 获取table中对应idx的block
func (table *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(table.sst.GetIndexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := table.blockCacheKey(idx)
	// 尝试从cache中获取blcok
	blk, ok := table.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var blockOffset pb.BlockOffset
	utils.CondPanic(!table.offsets(&blockOffset, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(blockOffset.GetOffset()),
	}

	var err error
	// 从mmapfile中读取到block的数据
	if b.data, err = table.read(b.offset, int(blockOffset.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			table.sst.GetFID(), b.offset, blockOffset.GetLen())
	}

	/*
		block 外 -> 内
		读取时会从外到内一层一层读取
		+---------------------------------------------------------------------+
		| checksum_len | checksum | entryOffset_len | entryOffset | Key-Value |
		+---------------------------------------------------------------------+
	*/
	readPos := len(b.data) - 4
	b.checkLen = int(utils.Bytes2Uint32(b.data[readPos : readPos+4]))
	if b.checkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.checkLen
	b.checksum = b.data[readPos : readPos+b.checkLen]

	b.data = b.data[:readPos]
	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	readPos = readPos - 4
	entryOffsts_len := utils.Bytes2Uint32(b.data[readPos : readPos+4])
	b.entriesIndexStart = readPos - (int(entryOffsts_len) * 4)
	entriesIndexEnd := b.entriesIndexStart + int(entryOffsts_len)*4
	b.entryOffsets = utils.Bytes2Uint32Slice(b.data[b.entriesIndexStart:entriesIndexEnd])

	table.lm.cache.blocks.Set(key, b)
	// k-vEntry在Block.data[:entriesIndexStart]
	return b, nil
}

// 	// 如果不在缓存中就要从sstable中获取
// 	var blockOffset pb.BlockOffset
// 	utils.CondPanic(!table.(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
// 	b=&block{
// 		// offset: ,
// 	}
// }

// // Searach从table中查找key
// func (table *table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
// 	/*
// 		外 --> 内
// 		+-------------------------------------------------------------------+
// 		| ckecksum_len | checksum | BlockIndexs_len | BlockIndexs | BlockData |
// 		+-------------------------------------------------------------------+
// 	*/
// 	table.IncrRef()
// 	defer table.DecrRef()
// 	index := table.sst.GetIndexs()
// 	bloomFilter := utils.Filter(index.BloomFilter)
// 	if table.sst.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
// 		return nil, utils.ErrKeyNotFound
// 	}

// 	// iter:=table.new
// }

// 创建迭代器
func (table *table) NewIterator(opt *utils.Options) utils.Iterator {
	table.IncrRef()
	return &tableIterator{
		opt:           opt,
		table:         table,
		blockIterator: &blockIterator{},
	}
}
