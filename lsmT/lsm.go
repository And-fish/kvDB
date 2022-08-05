package lsmt

import (
	"kvdb/utils"
)

type LSM struct {
	memtable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	maxMemFID  uint32
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

// 根据OPT创建新的LSM
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	// 初始化levelManager，包括
	lsm.levels = lsm.initLevelManager(opt)
	lsm.memtable, lsm.immutables = lsm.recovery()
	lsm.closer = utils.NewCloser()

	return lsm
}

// Close
func (lsm *LSM) Close() error {
	// 等待所有携程工作完毕
	lsm.closer.Close()

	if lsm.memtable != nil {
		if err := lsm.memtable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		immutable := lsm.immutables[i]
		if err := immutable.close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// StartCompacter 开始compact
func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors
	lsm.closer.Add(n) // 添加正在等待的协程
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i) // 启动n个compact协程
	}
}

// Rotate，将memtable添加到immutables中，再创建新的memtable
func (lsm *LSM) Rotate() {
	lsm.immutables = append(lsm.immutables, lsm.memtable)
	lsm.memtable = lsm.NewMemtable()
}

// Set
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 关闭
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	// 如果memtable满了会重新构建一个memtable
	if int64((lsm.memtable.wal.GetSize() + uint32(utils.EstimateWalCodecSize(entry)))) > lsm.option.MemTableSize {
		lsm.Rotate()
	}
	if err = lsm.memtable.set(entry); err != nil { // 添加到memtable中
		return
	}
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil { // 将immutable flush到L0层磁盘中
			return
		}

		err = immutable.close() // 回收掉wal文件
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	lsm.closer.Add(1)
	defer lsm.closer.Done()

	var entry *utils.Entry
	var err error
	if entry, err = lsm.memtable.Get(key); entry != nil && entry.Value != nil {
		return entry, err
	}
	// 从后往前查，因为后面的immutable被认为是更新的
	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.memtable.Get(key); entry != nil && entry.Value != nil {
			return entry, err
		}
	}
	// 从level manager中查询
	return lsm.levels.Get(key)
}

func (lsm *LSM) MemSize() int64 {
	return lsm.memtable.GetSize()
}
func (lsm *LSM) MemTableIsNil() bool {
	return lsm.memtable == nil
}
func (lsm *LSM) GetSkipListFromMemTable() *utils.SkipList {
	return lsm.memtable.sl
}
