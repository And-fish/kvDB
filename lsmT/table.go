package lsmt

import (
	"encoding/binary"
	"os"
	"strings"

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

// Delete删除对应的sstable.mmapfile
func (table *table) Delete() error {
	return table.sst.Delete()
}

// 用于set Blocks的Cache时用到的key
func (table *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(table.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(table.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

// 引用减一，表示本次不再引用；如果已经不被引用了，就会触发删除流程
func (table *table) DecrRef() error {
	ref := atomic.AddInt32(&table.ref, -1)
	// 如果这个table被没有被引用了，就会触发delete程序，
	// 首先会删除cache中对应的block信息
	// 再触发mmapfile的delete程序，会移除mmap的映射关系，再删除file
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

// 根据levelManager创建table
func openTable(lm *levelManager, tableName string, buider *tableBuilder) *table {
	// sstsize会尝试按照builder来定义，否则按照option来确定
	sstSize := int(lm.opt.SSTableMaxSz)
	if buider != nil {
		sstSize = int(buider.done().size)
	}

	var table *table
	var err error
	fid := utils.FID(tableName)
	// 如果传入了builder，说明需要根据将buider flush到磁盘
	if buider != nil {
		table, err = buider.flush(lm, tableName)
		if err != nil {
			utils.Err(err)
			return nil
		}
	} else { // 否则就尝试加载一个sst文件
		// 创建table
		table.lm = lm
		table.fid = fid
		// 初始化MmapFile
		table.sst = file.OpenSSTable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    sstSize,
		})
	}
	table.IncrRef()
	// 初始化sstable(根据的是sstable.file也就是MmapFile)
	if err := table.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	itr := table.NewIterator(&utils.Options{}) //默认IsAsc == false
	defer itr.Close()
	// 当前初始位置就是maxkey
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	table.sst.SetMaxKey(maxKey)

	return table
}

// 获取table的对应i的BlockOffset
func (table *table) offsets(BOffset *pb.BlockOffset, i int) bool {
	index := table.sst.GetIndexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		// 允许越界，table迭代器的优化
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
	// 如果不在block中，就要根据idx查询mmap中的block
	var blockOffset pb.BlockOffset
	// 如果idx合法，blokcOffset会保存对应idx的blockOffset
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
	readPos := len(b.data) - 4 // ckecksum_len大小为4bytes
	b.checkLen = int(utils.Bytes2Uint32(b.data[readPos : readPos+4]))
	if b.checkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.checkLen // ckecksum根据checksum_ken来确定
	b.checksum = b.data[readPos : readPos+b.checkLen]
	b.data = b.data[:readPos]
	// 校验checksum
	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	// entriesIndex_len的大小为4bytes
	readPos = readPos - 4
	entryOffsts_len := utils.Bytes2Uint32(b.data[readPos : readPos+4])
	b.entriesIndexStart = readPos - (int(entryOffsts_len) * 4)      // 每个entryOffset都是4bytes，所以要乘4
	entriesIndexEnd := b.entriesIndexStart + int(entryOffsts_len)*4 //readPos
	b.entryOffsets = utils.Bytes2Uint32Slice(b.data[b.entriesIndexStart:entriesIndexEnd])

	// 添加block到cache中
	table.lm.cache.blocks.Set(key, b)
	// k-vEntry在Block.data[:entriesIndexStart]
	return b, nil
}

// Searach从table中查找key
func (table *table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	/*
		外 --> 内
		+-------------------------------------------------------------------+
		| ckecksum_len | checksum | BlockIndexs_len | BlockIndexs | BlockData |
		+-------------------------------------------------------------------+
	*/
	table.IncrRef()
	defer table.DecrRef()
	// 首先要获取整个TableIndex(BlockIndex)，在sst.Init()被加载到到sst中 	(sst.initSSTable()中被初始化)
	index := table.sst.GetIndexs()
	bloomFilter := utils.Filter(index.BloomFilter) // 如果有bloomFilter，也会在sst.Init()中被unmarshal到sst中
	if table.sst.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound // 如果bloomFilter中没有就说明没有
	}

	// 创建一个新的tableIterator来实现查找流程
	iter := table.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.IsSameKey(iter.Item().Entry().Key, key) {
		if version := utils.ParseTimeStamp(iter.Item().Entry().Key); version > *maxVs {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

// 获取key-block entry
func (table *table) getEntry(key, block []byte, idx int) (entry *utils.Entry, err error) {
	if len(block) <= 0 {
		return nil, utils.ErrKeyNotFound
	}
	blockKey := string(block)
	blockKeys := strings.Split(blockKey, ",")
	if idx >= 0 && idx < len(blockKeys) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blockKeys[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

// 创建迭代器
func (table *table) NewIterator(opt *utils.Options) utils.Iterator {
	table.IncrRef()
	return &tableIterator{
		opt:           opt,
		table:         table,
		blockIterator: &blockIterator{},
	}
}

// Stale数据的大小
func (table *table) StaleDataSize() uint32 {
	return table.sst.GetIndexs().StaleDataSize
}
