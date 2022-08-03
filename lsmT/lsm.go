package lsmt

import (
	"bytes"
	"io/ioutil"
	"kvdb/file"
	"kvdb/utils"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type LSM struct {
	memtable  *memTable
	immutable []*memTable
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

func (lsm *LSM) Close() error {
	// 等待所有携程工作完毕
	lsm.closer.Close()

	if lsm.memtable != nil {
		if err := lsm.memtable.Close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutable {
		immutable := lsm.immutable[i]
		if err := immutable.Close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	return nil
}

// 通过fid打开memTable
func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	opt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_RDWR | os.O_CREATE,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: memTableFilePath(lsm.option.WorkDir, fid),
	}
	skiplist := utils.NewSkiplist(1 << 20)
	mt := &memTable{
		sl:  skiplist,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(opt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}

// 恢复memTbale
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// 从workDir下面获取所有的文件
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Err(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) { // 如果不是wal结尾的文件直接跳过
			continue
		}
		fileNameSz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fileNameSz-len(walFileExt)], 10, 64) // 将fileName转化为10进制的 uint64

		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}

	// 将fids排序
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	immuTables := []*memTable{}
	// 遍历所有的fid
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.GetSize() == 0 {
			continue
		}
		immuTables = append(immuTables, mt)
	}
	// 最后更新一下maxFid
	lsm.levels.maxFID = maxFid
	return lsm.NewMemtable(), immuTables
}

// 根据OPT创建新的LSM
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	// 初始化levelManager，包括
	lsm.levels = lsm.initLevelManager(opt)
	lsm.memtable, lsm.immutable = lsm.recovery()
	lsm.closer = utils.NewCloser()

	return lsm
}
