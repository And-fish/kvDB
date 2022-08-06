package kv

import (
	"hash/crc32"
	"kvdb/utils"
	"sync"
	"sync/atomic"
)

type request struct {
	Entries []*utils.Entry
	Ptrs    []*utils.ValuePtr
	Wg      sync.WaitGroup
	Err     error
	ref     int32
}

var requestPool = sync.Pool{
	New: func() any {
		return new(request)
	},
}

// 归0
func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Ptrs = req.Ptrs[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref = 0
}

// 引用加一
func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

// 引用减一
func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	// 如果没有被引用了就放回pool中
	req.Entries = nil
	requestPool.Put(req)
}

// 等待处理完成
func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}

// 返回request的大概size
func (req *request) estimateRequestSize() uint64 {
	size := uint64(0)
	for _, entry := range req.Entries {
		size += uint64(utils.MaxHeaderSize + len(entry.Key) + len(entry.Value) + crc32.Size)
	}
	return size
}

func (vlog *valueLog) newValuePtr(entry *utils.Entry) (*utils.ValuePtr, error) {
	req := requestPool.Get().(*request)
	req.reset()

	req.Entries = []*utils.Entry{entry}
	req.Wg.Add(1)
	req.IncrRef()
	defer req.DecrRef()
	err := vlog.write([]*request{req})
	return req.Ptrs[0], err
}
