package lsmt

import "kvdb/utils"

type LSM struct {
	memtable  *memtable
	immutable []*memtable
	levels    *levelManager
	option    *Options
	closer    *utils.Closer
	maxMemFID uint32
}
type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	DiscardStatsCh *chan map[uint32]int64
}

func (lsm *LSM) Close() {
	// 等待所有携程工作完毕
	lsm.closer.Close()

	if lsm.memtable != nil {

	}
}
