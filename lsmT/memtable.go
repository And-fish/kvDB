package lsmt

import (
	"bytes"
	"fmt"
	"kvdb/file"
	"kvdb/utils"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
)

const walFileExt string = ".wal"

type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

func memTableFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

// NewMemtable
func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      newFid,
		FileName: memTableFilePath(lsm.option.WorkDir, newFid),
	}
	return &memTable{
		lsm: lsm,
		wal: file.OpenWalFile(fileOpt),
		sl:  utils.NewSkiplist(int64(1 << 20)), // 1MB
	}
}

// Close
func (mt *memTable) Close() error {
	if err := mt.wal.Cloce(); err != nil {
		return err
	}
	return nil
}

// 向memTable中写入entry
func (mt *memTable) set(entry *utils.Entry) error {
	// 先写到wal中
	if err := mt.wal.Write(entry); err != nil {
		return err
	}
	// 再写道skiplist中
	mt.sl.Add(entry)
	return nil
}

// 从memTable中获取key对应的entry
func (mt *memTable) Get(key []byte) (*utils.Entry, error) {
	vs := mt.sl.Search(key) // 没找到会返回空的valueStruct

	return &utils.Entry{
		Key:     key,
		Value:   vs.Value,
		TTL:     vs.TTL,
		Meta:    vs.Meta,
		Version: vs.Version,
	}, nil
}

// 返回memTable的size
func (mt *memTable) GetSize() int64 {
	return mt.sl.GetSize()
}

// 返回的是一个更新memTable.maxVersion 和 添加 entry到skiplist的函数
func (mt *memTable) replayFunction(opt *Options) func(*utils.Entry, *utils.ValuePtr) error {
	return func(entry *utils.Entry, _ *utils.ValuePtr) error {
		if ts := utils.ParseTimeStamp(entry.Key); ts > mt.maxVersion {
			mt.maxVersion = ts
		}
		mt.sl.Add(entry)
		return nil
	}
}

// 更新skiplist
func (mt *memTable) UpdateSkipList() error {
	if mt.wal == nil || mt.sl == nil {
		return nil
	}
	// 这一段会将磁盘中的entry读取出来，再添加到skiplist中
	endOff, err := mt.wal.Iterate(true, 0, mt.replayFunction(mt.lsm.option)) // 返回的是mmap文件的读取endOffset
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", mt.wal.Name()))
	}
	return mt.wal.Truncate(int64(endOff)) // 截断文件
}
