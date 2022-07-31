package lsmt

import (
	"bytes"
	"kvdb/utils"
	"sync/atomic"
)

type memtable struct {
	lsm *LSM
	// wal *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

// NewMemtable
func (lsm *LSM) NewMemtable() *memtable {
	newFid := atomic.AddUint32(lsm.leb)
}
