package kv

import "kvdb/utils"

// Options corekv 总的配置文件
type Options struct {
	ValueThreshold      int64  // KV分离的阈值
	WorkDir             string // 数据库文件的保存目录
	MemTableSize        int64  // 内存MemTable的大小上限
	SSTableMaxSz        int64  // SST文件的大小上限
	MaxBatchCount       int64  // 批处理的最大处理次数
	MaxBatchSize        int64  // 批处理的最大处理大小
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

// // file.Options
// type Options struct {
// 	FID      uint64
// 	FileName string
// 	Dir      string
// 	Path     string
// 	Flag     int
// 	MaxSz    int
// }

// // lsmT.Options
// type Options struct {
// 	WorkDir      string
// 	MemTableSize int64
// 	SSTableMaxSz int64
// 	BlockSize int
// 	BloomFalsePositive float64

// 	NumCompactors       int
// 	BaseLevelSize       int64
// 	LevelSizeMultiplier int
// 	TableSizeMultiplier int
// 	BaseTableSize       int64
// 	NumLevelZeroTables  int
// 	MaxLevelNum         int

// 	DiscardStatsCh *chan map[uint32]int64
// }

// // utils.Options (iterator)
// type Options struct {
// 	Prefix []byte
// 	IsAsc  bool // 是否是升序查询
// }

// // Cache.options
// type Options struct {
// 	wlruPct uint8
// }

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{
		WorkDir:      "./work_test",
		MemTableSize: 1024,
		SSTableMaxSz: 1 << 30,
	}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
