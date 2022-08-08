package kv

import (
	"expvar"
	"fmt"
	lsmt "kvdb/lsmT"
	"kvdb/utils"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

var head = []byte("AND_FISH")

type KvAPI interface {
	Set(data *utils.Entry) error
	Get(key []byte) (*utils.Entry, error)
	Del(key []byte) error
	NewIterator(opt *utils.Options) utils.Iterator
	Info() *Stats
	Close() error
}

// 对外暴露的接口对象，全局唯一，持有各种资源句柄
type DB struct {
	sync.RWMutex
	opt   *Options
	lsm   *lsmt.LSM
	vlog  *valueLog
	stats *Stats

	flushCh     chan flushTask
	writeCh     chan *request
	blockWrites int32
	vptrHead    *utils.ValuePtr
	logRotates  int32
}

type flushTask struct {
	memTable     *utils.SkipList
	vptr         *utils.ValuePtr
	dropPrefixes [][]byte
}

func Open(opt *Options) *DB {
	closer := utils.NewCloser()
	db := &DB{
		opt: opt,
	}
	// 初始化vlog结构
	db.initVLog()
	db.lsm = lsmt.NewLSM(&lsmt.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
		DiscardStatsCh:      &(db.vlog.lfDiscardStats.flushCh), // 将lsm的DiscardStatsCh 和 vlog.lfDiscardStats.flushCh 连接起来，这样在compact的过程中就可以将过期key数据发送给vlog
	})

	db.stats = newStats(opt)
	go db.lsm.StartCompacter()

	db.writeCh = make(chan *request)
	db.flushCh = make(chan flushTask, 16)

	closer.Add(1)
	go db.runWrite(closer)
	go db.stats.StartStats()
	return db
}

func (db *DB) pushHead(ft flushTask) error {
	if ft.vptr.IsZero() {
		return errors.New("Head should not be zero")
	}
	// fmt.Printf("Storing value log head: %+v\n", ft.vptr)
	val := ft.vptr.Encode()
	haedTs := utils.KeyWithTS(head, uint64(time.Now().Unix()/1e9))
	ft.memTable.Add(&utils.Entry{
		Key:   haedTs,
		Value: val,
	})
	return nil
}

// 将request写入到LSM
func (db *DB) writeToLSM(req *request) error {
	if len(req.Ptrs) != len(req.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", req)
	}

	for i, entry := range req.Entries {
		if db.shouldWriteValueToLSM(entry) {
			entry.Meta = entry.Meta &^ utils.BitValuePointer // 取消 BitValuePointer 的标记
		} else {
			entry.Meta = entry.Meta | utils.BitValuePointer // 打上 BitValuePointer 的标记
			entry.Value = req.Ptrs[i].Encode()              // 编码为valuePtr
		}
		db.lsm.Set(entry)
	}
	return nil
}

// 写Requests
func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}
	// 处理回收函数
	done := func(err error) {
		for _, req := range reqs {
			req.Err = err
			req.Wg.Done()
		}
	}

	// 写入reqs到logFile中
	err := db.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}
	var count int
	for _, req := range reqs {
		if len(req.Entries) == 0 {
			continue
		}
		count += len(req.Entries)
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := db.writeToLSM(req); err != nil { // 写入到LSM中
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		db.Lock()
		db.updateHead(req.Ptrs)
		db.Unlock()
	}
	done(nil)
	return nil
}

// runWrite逻辑
func (db *DB) runWrite(c *utils.Closer) {
	defer c.Done() // 减少closer的一次引用
	pendingCh := make(chan struct{}, 1)

	// 执行写入reqs的逻辑
	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			utils.Err(fmt.Errorf("writeRequests: %v", err))
		}
		<-pendingCh // 写完后会消费pending的消息
	}

	reqsLen := new(expvar.Int)      // 用于追踪reqs数量
	reqs := make([]*request, 0, 10) // 用于保存reqs，batch模式
	for {
		var req *request
		select {
		case req = <-db.writeCh: // 如果有request发送到 writeCh，会处理后面的逻辑
		case <-c.CloseSignal: // 如果收到close通知，会跳转到close逻辑
			goto closedCase
		}
		// <-db.writeCh需要处理的逻辑
		for {
			// 会不断保存req
			reqs = append(reqs, req)
			reqsLen.Set(int64(len(reqs)))
			// 直到reqs数量超过阈值会主动触发write
			if len(reqs) >= 3*utils.KVWriteChCapacity {
				pendingCh <- struct{}{}
				goto writeCase
			}
			select {
			case req = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-c.CloseSignal: // 如果能接收到
				goto closedCase
			}
		}
		// close逻辑
	closedCase:
		for {
			// 会不断监听，如果能收到request，还是会append到reqs中；
			// 否则会发起一个pending通知，执行写入reqs后再返回
			select {
			case req = <-db.writeCh:
				reqs = append(reqs, req)
			default:
				pendingCh <- struct{}{}
				writeRequests(reqs)
				return
			}
		}
		// write逻辑
	writeCase:
		go writeRequests(reqs) // 启动一个协程去异步写入
		reqs = make([]*request, 0, 10)
		reqsLen.Set(0)
		// 执行完后触发for{}
	}

}

// Get
func (db *DB) Get(key []byte) (*utils.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}
	realKey := key

	var entry *utils.Entry
	var err error
	key = utils.KeyWithTS(key, math.MaxUint32)

	// 先从lsm中获取
	if entry, err = db.lsm.Get(key); err != nil {
		return entry, err
	}
	// 判断sst中存储的是不是valueptr
	if entry != nil && utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		// get valueptr
		vp.Decode(entry.Value)
		// 根据valueptr从vlog中获取数据
		valueBuf, unlockCallBack, err := db.vlog.read(&vp)
		defer utils.RunCallback(unlockCallBack)
		if err != nil {
			return nil, err
		}
		entry.Value = utils.SafeCopy(nil, valueBuf)
	}
	// 判断是否过期
	if lsmt.IsDeletedOrExpired(entry) {
		return nil, utils.ErrKeyNotFound
	}

	entry.Key = realKey
	return entry, nil
}

// Set添加写入entry
func (db *DB) Set(entry *utils.Entry) error {
	if entry == nil || len(entry.Key) == 0 {
		return utils.ErrEmptyKey
	}
	var vp *utils.ValuePtr
	var err error
	entry.Key = utils.KeyWithTS(entry.Key, math.MaxUint32)
	// 先写入到vlog中
	if !db.shouldWriteValueToLSM(entry) {
		if vp, err = db.vlog.newValuePtr(entry); err != nil {
			return err
		}
		entry.Meta |= utils.BitValuePointer // 打上标记
		entry.Value = vp.Encode()
	}
	return db.lsm.Set(entry) // set到lsm上

}

// 将待写的entries数据发送到chan write中
func (db *DB) sendToWriteCh(entries []*utils.Entry) (*request, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 { // 检查是否正在写 MEYBE
		return nil, utils.ErrBlockedWrites
	}
	var count, size int64
	// 计算这批entries中的总size和count
	for _, entry := range entries {
		size += int64(entry.EstimateSize(db.opt.ValueLogFileSize))
		count++
	}
	// 如果这次批处理超过了单次Batch的阈值，会返回错误
	if count >= db.opt.MaxBatchCount || size >= db.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	// 从pool中获取一个request
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()
	db.writeCh <- req // 通过channel发送
	return req, nil   // 向调用者返回request
}

// 判断是否应该set到lsm中
func (db *DB) shouldWriteValueToLSM(entry *utils.Entry) bool {
	return len(entry.Value) < int(db.opt.ValueThreshold)
}

// 删除key
func (db *DB) Del(key []byte) error {
	return db.Set(&utils.Entry{
		// value == nil会被认为是过期的
		Key:   key,
		Value: nil,
		TTL:   0,
	})
}

// 获取统计Info信息
func (db *DB) Info() *Stats {
	return db.stats
}

// Close
func (db *DB) Close() error {
	db.vlog.lfDiscardStats.closer.Close()
	if err := db.lsm.Close(); err != nil {
		return nil
	}
	if err := db.vlog.close(); err != nil {
		return nil
	}
	if err := db.stats.close(); err != nil {
		return nil
	}
	return nil
}

// 批写入，实际上就是封装了sendToWriteCh
func (db *DB) batchSet(entries []*utils.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	return req.Wait()
}

// Vlog_GC
func (db *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	headky := utils.KeyWithTS(head, math.MaxUint64)
	val, err := db.lsm.Get(headky)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = &utils.Entry{
				Key:   headky,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}

	var head utils.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	return db.vlog.runGC(discardRatio, &head)
}
