package kv

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"kvdb/file"
	"kvdb/utils"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const discardStatsFlushThreshold = 100 // DisCard的flush阈值

var lfDiscardStatsKey = []byte("!ByteDance!Please") // 用于Discead的固定引用字段

type valueLog struct {
	dirPath            string
	filesLock          sync.RWMutex
	filesMap           map[uint32]*file.LogFile
	maxFid             uint32 // vlog文件的fid,NOT SSTable FID
	filesToBeDeleted   []uint32
	numActiveIterators int32

	db                *DB
	writableLogOffset uint32
	numEntriesWritten uint32
	opt               Options

	garbageCh      chan struct{}
	lfDiscardStats *lfDiscardStats
}

type lfDiscardStats struct {
	sync.RWMutex
	m                 map[uint32]int64 // fid | 脏key数量
	flushCh           chan map[uint32]int64
	closer            *utils.Closer
	updatesSinceFlush int
}

// 用于重写log
type safeRead struct {
	k            []byte
	v            []byte
	recordOffset uint32
	lf           *file.LogFile
}

// 获取到写入的offset
func (vlog *valueLog) getWriteOffset() uint32 {
	return atomic.LoadUint32(&vlog.writableLogOffset)
}

// 删除LogFile
func (vlog *valueLog) deleteLogFile(lf *file.LogFile) error {
	if lf == nil {
		return nil
	}
	lf.Lock.Lock()
	defer lf.Lock.Unlock()
	utils.Err(lf.Close())
	return os.Remove(lf.FileName())
}

// 将reader中的数据转化为entry (decodeHeader报EOF，解析entry报utils.ErrTruncate)
func (sr *safeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	hr := utils.NewHashReader(reader)
	var h utils.Header
	hlen, err := h.DecodeFrom(hr) // 解码为header
	if err != nil {
		return nil, err
	}
	if h.KLen > uint32(1<<16) {
		return nil, utils.ErrTruncate
	}
	klen := h.KLen
	if cap(sr.k) < int(klen) {
		sr.k = make([]byte, 2*klen)
	}
	vlen := h.VLen
	if cap(sr.v) < int(vlen) {
		sr.v = make([]byte, 2*vlen)
	}

	entry := &utils.Entry{
		Offset: sr.recordOffset,
		Hlen:   hlen,
	}
	// 解析key
	buf := make([]byte, klen+vlen)
	if _, err := io.ReadFull(hr, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	entry.Key = buf[:klen]
	entry.Value = buf[klen:]

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(hr, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.Bytes2Uint32(crcBuf[:])
	if crc != hr.Sum32() {
		return nil, utils.ErrTruncate
	}
	entry.Meta = h.Meta
	entry.TTL = h.TTL
	return entry, nil
}

// 生成Vlog文件的filePath
func (vlog *valueLog) filePath(fid uint32) string {
	return utils.VlogFilePath(vlog.dirPath, fid)
}

// 统计脏数据
func (vlog *valueLog) getDiscardStats() error {
	key := utils.KeyWithTS(lfDiscardStatsKey, math.MaxUint64)
	var statsMap map[uint32]int64
	entry, err := vlog.db.Get(key)
	if err != nil {
		return err
	}
	if entry.Meta == 0 && len(entry.Value) == 0 {
		return nil
	}
	val := entry.Value

	if utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		vp.Decode(val)
		valBuf, unlockCallBack, err := vlog.read(&vp)
		val = utils.SafeCopy(nil, valBuf)
		utils.RunCallback(unlockCallBack)
		if err != nil {
			return err
		}
	}
	if len(val) == 0 {
		return nil
	}
	if len(val) == 0 {
		return nil
	}
	if err := json.Unmarshal(val, &statsMap); err != nil {
		return errors.Wrapf(err, "failed to unmarshal discard stats")
	}
	// fmt.Printf("Value Log Discard stats: %v\n", statsMap)
	vlog.lfDiscardStats.flushCh <- statsMap // 传入数据，在open()中启动一个协程执行flushDiscardStats()中被flush
	return nil
}

// 将Discard状态flush到磁盘中(引用前需要ADD CLOSER)
func (vlog *valueLog) flushDiscardStats() {
	defer vlog.lfDiscardStats.closer.Done() // 这一步是go携程异步的，在引用函数时Add了 closer

	// 这个函数会将discard的entry输出为json []byte形式
	mergeStatsFunc := func(stats map[uint32]int64) ([]byte, error) {
		vlog.lfDiscardStats.Lock()
		defer vlog.lfDiscardStats.Unlock()

		for fid, count := range stats {
			vlog.lfDiscardStats.m[fid] += count
			vlog.lfDiscardStats.updatesSinceFlush++
		}

		if vlog.lfDiscardStats.updatesSinceFlush <= discardStatsFlushThreshold {
			// 如果没有到达flush阈值就直接返回
			return nil, nil
		}
		// 如果超过了阈值就会将需要flush的值输出为json格式
		encodeDs, err := json.Marshal(vlog.lfDiscardStats.m) // 加码
		if err != nil {
			return nil, err
		}
		vlog.lfDiscardStats.updatesSinceFlush = 0
		return encodeDs, nil
	}

	// 这个函数会将merge后的状态信息作为内部key写入到LSM中
	process := func(stats map[uint32]int64) error {
		encodeDs, err := mergeStatsFunc(stats) // 这里调用会获取到json形式的discard数据
		if err != nil || encodeDs == nil {
			return err
		}

		entries := []*utils.Entry{{ // 第一个entry是固定的引用entry，key是固定的，value是discard数据Json格式
			Key:   utils.KeyWithTS(lfDiscardStatsKey, 1),
			Value: encodeDs,
		}}
		req, err := vlog.db.sendToWriteCh(entries) // 将entries处理为request，并发送出去处理
		if err != nil {
			return errors.Wrapf(err, "failed to push discard stats to write channel")
		}
		return req.Wait() // 等待request处理结束
	}

	closer := vlog.lfDiscardStats.closer
	for { // 不断循环监听channel
		select {
		case <-closer.CloseSignal:
			// 如果收到了close信号就结束
			return
		case stats := <-vlog.lfDiscardStats.flushCh:
			// 如果收到了传来的flushStatus，就会启动处理函数
			if err := process(stats); err != nil {
				utils.Err(fmt.Errorf("unable to process discardstats with error: %s", err))
			}
			// 如果在收到close之前会阻塞在接受flushCh，收到flushCh会重复循环监听
		}
	}
}

// TODO：是否可以将扫描workdir的操作集合起来
// 初始化FilesMap，扫描workDir下面的vlog文件，并持有到句柄
func (vlog *valueLog) initFilesMap() error {
	vlog.filesMap = make(map[uint32]*file.LogFile) //fileMap是记录workDir下面所有vlog信息的

	files, err := ioutil.ReadDir(vlog.dirPath)
	if err != nil {
		return utils.WarpErr(fmt.Sprintf("Unable to open log dir. path[%s]", vlog.dirPath), err)
	}

	foundMap := make(map[uint64]struct{})
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".vlog") {
			continue
		}
		fNameSz := len(f.Name())
		fid, err := strconv.ParseUint(f.Name()[:fNameSz-5], 10, 32) // 截取前面的fid
		if err != nil {
			return utils.WarpErr(fmt.Sprintf("Unable to parse log id. name:[%s]", f.Name()), err)
		}
		if _, ok := foundMap[fid]; ok {
			// dir下不应该有重复fid的vlog文件
			return utils.WarpErr(fmt.Sprintf("Duplicate file found. Please delete one. name:[%s]", f.Name()), err)
		}
		foundMap[fid] = struct{}{}

		lf := &file.LogFile{
			FID:  uint32(fid),
			Lock: sync.RWMutex{},
		}
		vlog.filesMap[uint32(fid)] = lf
		if vlog.maxFid < uint32(fid) {
			vlog.maxFid = uint32(fid)
		}
	}
	return nil
}

// 创建VolgFile (会初始化Volg.writableLogOffset 和 Volg.numEntriesWritten)
func (vlog *valueLog) createVlogFile(fid uint32) (*file.LogFile, error) {
	path := vlog.filePath(fid)

	// 应该可以直接取出，待测试
	lf := &file.LogFile{
		FID:  fid,
		Lock: sync.RWMutex{},
	}
	var err error
	// 创建文件
	utils.Panic2(nil, lf.Open(&file.Options{
		FID:      uint64(fid),
		FileName: path,
		Dir:      vlog.dirPath,
		Path:     vlog.dirPath,
		MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
	}))

	// 错误处理函数
	removeFile := func() {
		utils.Err(os.Remove(lf.FileName()))
	}

	// 可以将Success做成一个Hook？
	// if err = lf.Success(); err != nil {
	// 	removeFile()
	// 	return nil, err
	// }

	if err = utils.SyncDir(vlog.dirPath); err != nil {
		removeFile()
		return nil, utils.WarpErr(fmt.Sprintf("Sync value log dir[%s]", vlog.dirPath), err)
	}

	// 到这说明已经成功sync到磁盘了，可以写入到内存中vlog
	vlog.filesLock.Lock()
	vlog.filesMap[fid] = lf
	vlog.maxFid = fid
	// 新文件的writeOffset == 0
	atomic.StoreUint32(&vlog.writableLogOffset, utils.VlogHeaderSize)
	vlog.numEntriesWritten = 0
	vlog.filesLock.Unlock()
	return lf, nil
}

// 获取可用的fids数组并按照fid从小到大排序
func (vlog *valueLog) sortedFids() []uint32 {
	toBeDeleted := make(map[uint32]struct{})
	for _, fid := range vlog.filesToBeDeleted {
		toBeDeleted[fid] = struct{}{}
	}
	res := make([]uint32, 0, len(vlog.filesMap))
	for fid := range vlog.filesMap {
		if _, ok := toBeDeleted[fid]; !ok { // 过滤掉正在被删除的文件
			res = append(res, fid)
		}
	}
	// 按照fid大小，从小到大排序
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	return res
}

// 从后往前找到一个可用的ValuePtr p，更新db.vptrHead == p
func (db *DB) updateHead(vps []*utils.ValuePtr) {
	var vp *utils.ValuePtr
	for i := len(vps) - 1; i >= 0; i-- {
		p := vps[i]
		if !p.IsZero() {
			vp = p
			break
		}
	}
	if vp.IsZero() {
		return
	}
	// vp应该要比db.vptrHea更新
	utils.CondPanic(vp.Less(db.vptrHead), fmt.Errorf("ptr.Less(db.vhead) is true"))
	db.vptrHead = vp
}

// 重放函数，将entry的key 和 vp表示的value重写到LSM中
func (db *DB) replayFunction() func(*utils.Entry, *utils.ValuePtr) error {
	// 这个函数会向LSM中插入kv
	setLSM := func(key []byte, vs utils.ValueStruct) {
		db.lsm.Set(&utils.Entry{
			Key:   key,
			Value: vs.Value,
			TTL:   vs.TTL,
			Meta:  vs.Meta,
		})
	}

	return func(entry *utils.Entry, vp *utils.ValuePtr) error {
		key := make([]byte, len(entry.Key))
		copy(key, entry.Key)
		var val []byte
		meta := entry.Meta
		if db.shouldWriteValueToLSM(entry) { // 如果可以直接插入到LSM，value是原value
			val = make([]byte, len(entry.Value))
			copy(val, entry.Value)
		} else { // 如果不可用插入到LSM，value是vp.encode
			val = vp.Encode()
			meta = meta | utils.BitValuePointer
		}
		db.updateHead([]*utils.ValuePtr{vp}) // 将vp置为db.vptrHead
		v := utils.ValueStruct{
			Value: val,
			Meta:  meta,
			TTL:   entry.TTL,
		}
		setLSM(key, v)
		return nil
	}
}

// interate是为用于处理logfile中所有entry的统一接口，可以实现对每一个entry都执行一次fn
func (vlog *valueLog) iterate(lf *file.LogFile, offset uint32, fn utils.LogEntry) (uint32, error) {
	if offset == 0 {
		offset = utils.VlogHeaderSize
	}
	if offset == uint32(lf.Size()) {
		return offset, nil
	}

	// 从offset开始读
	if _, err := lf.Seek(int64(offset), io.SeekStart); err != nil {
		return 0, errors.Wrapf(err, "Unable to seek, name:%s", lf.FileName())
	}

	reader := bufio.NewReader(lf.FD())
	read := &safeRead{
		k:            make([]byte, 10),
		v:            make([]byte, 10),
		recordOffset: offset,
		lf:           lf,
	}
	var validEndOffset uint32 = offset

loop:
	for { // 一个vlog中可能会有多个entry，所以会循环读
		entry, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop // 读完了就跳出循环
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case entry == nil:
			continue
		}

		var vp utils.ValuePtr
		vp.Len = uint32(entry.Hlen + len(entry.Key) + len(entry.Value) + crc32.Size)
		read.recordOffset += vp.Len

		vp.Offset = entry.Offset
		vp.Fid = lf.FID
		validEndOffset = read.recordOffset
		if err := fn(entry, &vp); err != nil { // 对entry执行函数
			if err == utils.ErrStop { // fn可以通过返回 err == utils.ErrStop来暂停处理
				break
			}
			return 0, utils.WarpErr(fmt.Sprintf("Iteration function %s", lf.FileName()), err)
		}
	}
	return validEndOffset, nil
}

// 封装interate，循环对lf中所有的entry执行repalyFn(将所有entry重新写入到LSM中)，并处理异常lf(修正lf大小)
func (vlog *valueLog) replayLog(lf *file.LogFile, offset uint32, repalyFn utils.LogEntry) error {
	// 对这个logFile执行重写函数，将所有entry重新写入到LSM中
	endOffset, err := vlog.iterate(lf, offset, repalyFn) // 对lf中所有的entry执行repalyFn，offset是第一个entry所在的位置

	if err != nil {
		return errors.Wrapf(err, "Unable to replay logfile:[%s]", lf.FileName())
	}
	if int64(endOffset) == int64(lf.Size()) { // 如果文件执行到了尾部，返回
		return nil
	}

	// 如果当前lf不是最新的vlog文件(正在被写入的)，且 endOffset <= utils.VlogHeaderSize，应该被删除
	if endOffset <= utils.VlogHeaderSize {
		if lf.FID != vlog.maxFid {
			return utils.ErrDeleteVlogFile
		}
		// return lf.Seccess()
		return nil
	}
	// fmt.Printf("Truncating vlog file %s to offset: %d\n", lf.FileName(), endOffset)
	// 走到这一步说明要截取为对应大小
	if err = lf.Truncate(int64(endOffset)); err != nil {
		return utils.WarpErr(fmt.Sprintf("Truncation needed at offset %d. Can be done manually as well.", endOffset), err)
	}
	return nil
}

// 重放，从vp开始将vlog文件加载到LSM中
func (vlog *valueLog) open(db *DB, p *utils.ValuePtr, replayFn utils.LogEntry) error {
	vlog.lfDiscardStats.closer.Add(1)
	go vlog.flushDiscardStats() // 启动一个协程去处理可能需要flush的discard数据

	// 扫描workdir下面所有的vlog文件，并加载到filemap中
	if err := vlog.initFilesMap(); err != nil {
		return err
	}

	if len(vlog.filesMap) == 0 {
		// 如果没有就创建0号vlog
		_, err := vlog.createVlogFile(0)
		return utils.WarpErr("Error while creating log file in valueLog.open", err)
	}
	fids := vlog.sortedFids()  // 获取到所有的fid，并按照fid从小到大排序
	for _, fid := range fids { // 循环遍历fid，从fid小的开始
		lf, ok := vlog.filesMap[fid]
		utils.CondPanic(!ok, fmt.Errorf("vlog.filesMap[fid] fid not found"))
		var err error
		if err = lf.Open(&file.Options{ // 打开vlog文件，加载vlog中的数据到内存中，
			// 待修改(0号vlog被初始化了两次)
			FID:      uint64(fid),
			FileName: vlog.filePath(fid),
			Dir:      vlog.dirPath,
			Path:     vlog.dirPath,
			MaxSz:    2 * vlog.db.opt.ValueLogFileSize,
		}); err != nil {
			return errors.Wrapf(err, "Open existing file: %q", lf.FileName())
		}

		var offset uint32
		if fid == p.Fid { // 如果当前vlog和dbHead一样，就会从head后面开始
			offset = p.Offset + p.Len
		}
		// fmt.Printf("Replaying file id: %d at offset: %d\n", fid, offset)
		// now := time.Now()
		// 执行重写函数，将lf中的entries重写写入到LSM中，再修正lf的大小
		if err := vlog.replayLog(lf, offset, replayFn); err != nil {
			if err == utils.ErrDeleteVlogFile { // 如果该lf需要删除
				delete(vlog.filesMap, fid)         // 先删除内存中的索引
				if err := lf.Close(); err != nil { // close掉logFile(删除)
					return errors.Wrapf(err, "failed to close vlog file %s", lf.FileName())
				}
				path := vlog.filePath(lf.FID)
				if err := os.Remove(path); err != nil {
					return errors.Wrapf(err, "failed to delete empty value log file: %q", path)
				}
				continue // 如果重写的时候遇到需要删除的，删除之后会处理下一个logfile
			}
			return err
		}
		// fmt.Printf("Replay took: %s\n", time.Since(now))

		// 只有head之后的才需要重放？可能可以这样实现
		// var offset uint32
		// if fid >= p.Fid {
		// 	offset = p.Offset + p.Len
		// 	if err := vlog.replayLog(lf, offset, replayFn); err != nil {
		// 		if err == utils.ErrDeleteVlogFile {
		// 			delete(vlog.filesMap, fid)
		// 			if err := lf.Close(); err != nil {
		// 				return errors.Wrapf(err, "failed to close vlog file %s", lf.FileName())
		// 			}
		// 			path := vlog.filePath(lf.FID)
		// 			if err := os.Remove(path); err != nil {
		// 				return errors.Wrapf(err, "failed to delete empty value log file: %q", path)
		// 			}
		// 			continue
		// 		}
		// 		return err
		// 	}
		// }

		// Init()是一个扩展方向，比如预加载一些数据
		if fid < vlog.maxFid { // 如果本次处理的logfile不是最新的logfile，会重载一次size
			if err := lf.Init(); err != nil {
				return err
			}
		}
	}

	// 到这，获取到最后一个vlog，也就是活跃的file
	last, ok := vlog.filesMap[vlog.maxFid]
	utils.CondPanic(!ok, errors.New("vlog.filesMap[vlog.maxFid] not found"))
	lastOffset, err := last.Seek(0, io.SeekEnd) // offset是最后一个文件的尾部+0
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("file.Seek to end path:[%s]", last.FileName()))
	}
	vlog.writableLogOffset = uint32(lastOffset)

	// 将head更新到最新的vlog的offset 0位置
	vlog.db.vptrHead = &utils.ValuePtr{
		Fid:    vlog.maxFid,
		Offset: uint32(lastOffset),
	}
	// 获取一下过期数据
	if err := vlog.getDiscardStats(); err != nil {
		fmt.Errorf("Failed to populate discard stats: %s\n", err)
	}
	return nil
}

// 重放机制会从head所在的位置开始
func (db *DB) getHead() (*utils.ValuePtr, uint64) {
	var vptr utils.ValuePtr
	return &vptr, 0
}

// initVLog，vlog的入口函数
func (db *DB) initVLog() {
	vptr, _ := db.getHead() // head作为一个重放的checkPoint存在
	vlog := &valueLog{
		dirPath:          db.opt.WorkDir,
		filesToBeDeleted: make([]uint32, 0),
		lfDiscardStats: &lfDiscardStats{ // 过期vlog状态
			m:       make(map[uint32]int64),
			flushCh: make(chan map[uint32]int64, 16),
			closer:  utils.NewCloser(),
		},
	}

	vlog.db = db
	vlog.opt = *db.opt
	vlog.garbageCh = make(chan struct{}, 1) // GC锁，保证只有一个协程在处理gc
	// 重放机制，保证vlog文件和LSM的一致性
	if err := vlog.open(db, vptr, db.replayFunction()); err != nil {
		utils.Panic(err)
	}
	db.vlog = vlog
}

// 根据valueptr读取到logFile文件(会加读锁)
func (vlog *valueLog) getFileRLocked(vp *utils.ValuePtr) (*file.LogFile, error) {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()

	lf, ok := vlog.filesMap[vp.Fid]
	if !ok {
		return nil, errors.Errorf("file with ID: %d not found", vp.Fid)
	}

	maxFid := vlog.maxFid
	if vp.Fid == maxFid {
		currentOffset := vlog.getWriteOffset()
		if vp.Offset >= currentOffset {
			return nil, errors.Errorf("Invalid value pointer offset: %d greater than current offset: %d", vp.Offset, currentOffset)
		}
	}

	lf.Lock.RLock()
	return lf, nil
}

// 从vlog中获取到valuePtr对应的数据
func (vlog *valueLog) readValueBytes(vp *utils.ValuePtr) ([]byte, *file.LogFile, error) {
	lf, err := vlog.getFileRLocked(vp)
	if err != nil {
		return nil, nil, err
	}
	buf, err := lf.Read(vp)
	return buf, lf, err
}

// 解读锁，方便后续扩展
func (vlog *valueLog) getUnlockCallback(lf *file.LogFile) func() {
	if lf == nil {
		return nil
	}
	// 这里是否可以做一些处理函数
	return lf.Lock.RUnlock
}

// 从vlog读取数据，返回value对应的buf / 解锁callback
func (vlog *valueLog) read(vp *utils.ValuePtr) ([]byte, func(), error) {
	buf, lf, err := vlog.readValueBytes(vp)
	unlockCallBack := vlog.getUnlockCallback(lf)
	if err != nil {
		return nil, unlockCallBack, err
	}
	if vlog.opt.VerifyValueChecksum {
		hash := crc32.New(utils.CastagnoliCrcTable)
		if _, err := hash.Write(buf[:len(buf)-crc32.Size]); err != nil {
			utils.RunCallback(unlockCallBack)
			return nil, nil, errors.Wrapf(err, "failed to write hash for vp %+v", vp)
		}
		checksum := buf[len(buf)-crc32.Size:]
		if hash.Sum32() != utils.Bytes2Uint32(checksum) {
			utils.RunCallback(unlockCallBack)
			return nil, nil, errors.Wrapf(utils.ErrChecksumMismatch, "value corrupted for vp: %+v", vp)
		}
	}

	var h utils.Header
	headerLen := h.Decoder(buf)
	kv := buf[headerLen:]
	if uint32(len(kv)) < h.KLen+h.VLen {
		fmt.Errorf("Invalid read: vp: %+v\n", vp)
		return nil, nil, errors.Errorf("Invalid read: Len: %d read at:[%d:%d]",
			len(kv), h.KLen, h.KLen+h.VLen)
	}

	return kv[h.KLen : h.KLen+h.VLen], unlockCallBack, nil
}

// check all request是否越界
func (vlog *valueLog) validateWrites(reqs []*request) error {
	wOffset := uint64(vlog.getWriteOffset()) // 原子获取
	for _, req := range reqs {
		size := req.estimateRequestSize()
		vlogOffset := wOffset + size
		if vlogOffset > uint64(utils.MaxVlogFileSize) { // 不能超过uint32
			return errors.Errorf("Request size offset %d is bigger than maximum offset %d",
				vlogOffset, utils.MaxVlogFileSize)
		}
		if vlogOffset >= uint64(vlog.opt.ValueLogFileSize) { //  1<<20 < vlogOffset < maxuiny32会将下一个写入到新的vlog
			wOffset = 0
			continue
		}
		wOffset = vlogOffset
	}
	return nil
}

// 将收到的[]*request写入到logFile中，会创建reqs的vptr
func (vlog *valueLog) write(reqs []*request) error {
	// check能否写入
	if err := vlog.validateWrites(reqs); err != nil {
		return err
	}

	vlog.filesLock.RLock()
	maxfid := vlog.maxFid
	lf := vlog.filesMap[maxfid]
	vlog.filesLock.RUnlock()
	var buf bytes.Buffer

	// 这个函数执行后会将buf中的数据读出来写入到mmap
	flushWriteFunc := func() error {
		if buf.Len() == 0 {
			return nil
		}
		data := buf.Bytes() // 全部读出
		offset := vlog.getWriteOffset()
		if err := lf.Write(offset, data); err != nil { // 写入到mmap中
			return errors.Wrapf(err, "Unable to write to value log file: %q", lf.FileName())
		}
		buf.Reset() // Reset buf
		atomic.AddUint32(&vlog.writableLogOffset, uint32(len(data)))
		lf.SetSize(vlog.writableLogOffset)
		return nil
	}

	// 这个函数会执行flushWriteFunc()将buf写入到磁盘中，如果写完后vlog超过了options值，会重新创建一个vlog
	toDisk := func() error {
		if err := flushWriteFunc(); err != nil { // 调用上面声明的函数，会将buf中的数据追加写入
			return err
		}
		// 如果数量或者大小超过了阈值会切分vlog	(超过max值的reqs在 validateWrites() 被拒绝了)
		if vlog.getWriteOffset() > uint32(vlog.opt.ValueLogFileSize) || vlog.numEntriesWritten > vlog.opt.ValueLogMaxEntries {
			if err := lf.DoneWriting(vlog.getWriteOffset()); err != nil { // 截断文件
				return err
			}
			newfid := atomic.AddUint32(&vlog.maxFid, 1)
			utils.CondPanic(newfid <= 0, fmt.Errorf("newid has overflown uint32: %v", newfid))
			newlf, err := vlog.createVlogFile(newfid)
			if err != nil {
				return err
			}
			lf = newlf
			atomic.AddInt32(&vlog.db.logRotates, 1)
		}
		return nil
	}

	// 循环所有request
	for i := range reqs {
		req := reqs[i]
		req.Ptrs = req.Ptrs[:0]
		var wCount int
		for j := range req.Entries { // 循环request中所有的entries
			entry := req.Entries[j]
			if vlog.db.shouldWriteValueToLSM(entry) { // 如果不需要存储到vlog中，就append一个空vp
				req.Ptrs = append(req.Ptrs, &utils.ValuePtr{})
				continue
			}
			var vp utils.ValuePtr
			vp.Fid = lf.FID
			vp.Offset = vlog.getWriteOffset() + uint32(buf.Len()) // 第一次buf.Len应该为0
			plen, err := lf.EncodeEntry(entry, &buf, vp.Offset)   // 将entry编码到buf中
			if err != nil {
				return err
			}
			vp.Len = uint32(plen)
			req.Ptrs = append(req.Ptrs, &vp)
			wCount++

			if buf.Len() > vlog.db.opt.ValueLogFileSize {
				if err := flushWriteFunc(); err != nil {
					return err
				}
			}
		}
		vlog.numEntriesWritten += uint32(wCount)

		writeNow :=
			vlog.getWriteOffset()+uint32(buf.Len()) > uint32(vlog.opt.ValueLogFileSize) ||
				vlog.numEntriesWritten > uint32(vlog.opt.ValueLogMaxEntries)
		if writeNow {
			if err := toDisk(); err != nil { //
				return err
			}
		}
	}
	return toDisk() // 最后再尝试save一次
}

// close
func (vlog *valueLog) close() error {
	if vlog == nil || vlog.db == nil {
		return nil
	}
	<-vlog.lfDiscardStats.closer.CloseSignal
	var err error
	for fid, lf := range vlog.filesMap {
		lf.Lock.Lock()
		maxfid := vlog.maxFid
		if fid == maxfid {
			if truncErr := lf.Truncate(int64(vlog.getWriteOffset())); truncErr != nil {
				err = truncErr
			}
		}
		if closeErr := lf.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		lf.Lock.Unlock()
	}
	return err
}

// sampler采样器
type sampler struct {
	lf            *file.LogFile
	sizeRatio     float64
	countRatio    float64
	fromBeginning bool
}

type reason struct {
	total   float64
	discard float64
	count   int
}

func (vlog *valueLog) sample(s *sampler, discardRatio float64) (*reason, error) {
	sizeP := s.sizeRatio
	countP := s.countRatio
	fileSize := s.lf.Size()

	sizeN := sizeP * float64(fileSize)
	SizeM := sizeN / (1 << 20) // 多少MB
	countN := int(countP * float64(vlog.opt.ValueLogMaxEntries))

	var skipFirstM float64
	var err error
	// 如果fromBeginning == false，会随机采样其实地址
	if !s.fromBeginning {
		skipFirstM = float64(rand.Int63n(fileSize))
		skipFirstM -= sizeN
		skipFirstM /= (1 << 20)
	}
	var skipped float64

	var r reason
	start := time.Now()
	var numIterations int
	// 用于iterate()的处理函数
	fn := func(entry *utils.Entry, vp *utils.ValuePtr) error {
		numIterations++ // 迭代次数 +1
		entrySz := float64(vp.Len) / (1 << 20)
		if skipped < skipFirstM { // 如果还没有到(随机的)跳转位置，就会return取消对本次entry的处理
			skipped += entrySz
			return nil
		}
		if r.count > countN { // 如果超过了计数阈值，会stop
			return utils.ErrStop
		}
		if r.total > SizeM { // 如果超过了大小阈值，会stop
			return utils.ErrStop
		}
		if time.Since(start) > 10*time.Second { // 如果时间超过了十秒，会stop
			return utils.ErrStop
		}

		r.total += entrySz // ADD size窗口
		r.count += 1       // ADD count窗口
		// entry, err := vlog.db.Get(entry.Key) // 获取到entry
		if err != nil {
			return err
		}
		if utils.DiscardEntry(entry) {
			r.discard += entrySz
			return nil
		}
		utils.CondPanic(len(entry.Value) <= 0, fmt.Errorf("len(entry.Value) <= 0"))
		vp.Decode(entry.Value)
		if vp.Fid > s.lf.FID {
			r.discard += entrySz
			return nil
		}
		if vp.Offset > entry.Offset {
			r.discard += entrySz
			return nil
		}
		return nil
	}
	// 对lf中所有的entry都执行fn
	_, err = vlog.iterate(s.lf, 0, fn)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Fid: %d. Skipped: %5.2fMB Num iterations: %d. Data status=%+v\n", s.lf.FID, skipped, numIterations, r)

	// 如果count没有到达阈值，且总大小小于size窗口的75%，则认为采样失败，不考虑本次采样的结果
	// 或者discard数量小于窗口的（discard容忍度），也会认为采样失败
	if (r.count < countN && r.total < SizeM*0.75) || r.discard < discardRatio*r.total {
		fmt.Printf("Skipping GC on fid: %d", s.lf.FID)
		return nil, utils.ErrNoRewrite
	}
	return &r, nil
}

// /////// GC
// 找到需要gc的vlog文件
func (vlog *valueLog) pickLog(head *utils.ValuePtr) []*file.LogFile {
	vlog.filesLock.RLock()
	defer vlog.filesLock.RUnlock()

	files := make([]*file.LogFile, 0)
	fids := vlog.sortedFids() // 拿到所有的fids
	switch {
	case len(fids) <= 1:
		return nil
	}

	// 候选log
	candidate := struct {
		fid     uint32
		discard int64
	}{math.MaxUint32, 0}

	vlog.lfDiscardStats.RLock()
	// 遍历fids找到一个小于等于head的 && discard数量最大的logFile
	for _, fid := range fids {
		if fid >= head.Fid {
			break
		}
		if vlog.lfDiscardStats.m[fid] > candidate.discard {
			candidate.fid = fid
			candidate.discard = vlog.lfDiscardStats.m[fid]
		}
	}
	vlog.lfDiscardStats.RUnlock()

	// 如果找到某个合法的candidate，将待清理的logfile append进去
	if candidate.fid != math.MaxUint32 {
		files = append(files, vlog.filesMap[candidate.fid])
	}

	// 再补充一个随机的fid作为GC对象，防止统计discard不充分的情况，保证files中存在对象
	var idxHead int
	for i, fid := range fids {
		if fid == head.Fid {
			idxHead = i
			break
		}
	}
	if idxHead == 0 {
		idxHead = 1
	}
	idx := rand.Intn(idxHead)
	if idx > 0 {
		idx = rand.Intn(idx - 1)
	}
	files = append(files, vlog.filesMap[fids[idx]])
	return files
}

func (vlog *valueLog) iteratorCount() int {
	return int(atomic.LoadInt32(&vlog.numActiveIterators))
}

// 重写操作，只负责vlog的重写
func (vlog *valueLog) rewrite(lf *file.LogFile) error {
	vlog.filesLock.RLock()
	maxfid := vlog.maxFid
	vlog.filesLock.RUnlock()
	utils.CondPanic(uint32(lf.FID) >= maxfid, fmt.Errorf("fid to move: %d. Current max fid: %d", lf.FID, maxfid))

	es := make([]*utils.Entry, 0, 1000)
	var size int64
	var count, moved int
	fn := func(entry *utils.Entry) error {
		count++
		if count%100000 == 0 {
			fmt.Printf("Processing entry %d\n", count)
		}
		lsmEntry, err := vlog.db.lsm.Get(entry.Key) // 获取到sst文件中的entry
		if err != nil {
			return err
		}
		if utils.DiscardEntry(lsmEntry) { // 如果过期了就不用重写
			return nil
		}
		if len(lsmEntry.Value) == 0 {
			return errors.Errorf("Empty value: %+v", lsmEntry)
		}
		var vp utils.ValuePtr
		vp.Decode(lsmEntry.Value) // 解析sst中entry的value为vp

		if vp.Fid > lf.FID {
			return nil
		}
		if vp.Offset > entry.Offset { // 如果sst中entry记录的offset超过了lf中解析出来的的offset，跳过
			return nil
		}
		if vp.Fid == lf.FID && vp.Offset == entry.Offset {
			// 如果读取到的entry相同
			moved++
			ne := new(utils.Entry)
			ne.Meta = 0
			ne.TTL = entry.TTL
			ne.Key = append([]byte{}, entry.Key...)
			ne.Value = append([]byte{}, entry.Value...) // newEntry的Value是从vlog解析出来的实际的value
			sz := int64(ne.EstimateSize(vlog.db.opt.ValueLogFileSize))
			sz += int64(len(entry.Value))

			if int64(len(es)+1) >= vlog.opt.MaxBatchCount || size+sz >= NewDefaultOptions().MaxBatchSize {
				if err := vlog.db.batchSet(es); err != nil {
					return err
				}
				size = 0
				es = es[:0]
			}
			es = append(es, ne)
			size += sz
		}
		return nil
	}

	_, err := vlog.iterate(lf, 0, func(entry *utils.Entry, vp *utils.ValuePtr) error {
		return fn(entry)
	})

	if err != nil {
		return err
	}
	// 处理es中剩下的数据
	batchSize := 1024
	var loops int
	for i := 0; i < len(es); {
		loops++
		if batchSize == 0 {
			return utils.ErrNoRewrite
		}
		end := i + batchSize
		if end > len(es) {
			end = len(es)
		}
		if err := vlog.db.batchSet(es[i:end]); err != nil {
			if err == utils.ErrTxnTooBig {
				batchSize /= 2
				continue
			}
			return err
		}
		i += batchSize
	}

	var deleteFileNow bool

	{
		vlog.filesLock.Lock()
		if _, ok := vlog.filesMap[lf.FID]; !ok {
			vlog.filesLock.Unlock()
			return errors.Errorf("Unable to find fid: %d", lf.FID)
		}
		if vlog.iteratorCount() == 0 {
			// fmt.Println(lf.FID)
			// delete(vlog.filesMap, lf.FID)
			// deleteFileNow = true
		} else {
			vlog.filesToBeDeleted = append(vlog.filesToBeDeleted, lf.FID)
		}
		vlog.filesLock.Unlock()
	}

	if deleteFileNow {
		if err := vlog.deleteLogFile(lf); err != nil {
			return err
		}
	}
	return nil
}

// 执行GC操作
func (vlog *valueLog) doRunGc(lf *file.LogFile, discardRatio float64) (err error) {
	defer func() {
		// 退出时需要移除掉本次的数据
		if err == nil {
			vlog.lfDiscardStats.Lock()
			delete(vlog.lfDiscardStats.m, lf.FID)
			vlog.lfDiscardStats.Unlock()
		}
	}()

	s := &sampler{
		lf:            lf,
		countRatio:    0.01, // 采样窗口为1%的ValueLogMaxEntries
		sizeRatio:     0.1,  // 采样窗口为10%总size
		fromBeginning: false,
	}
	if _, err = vlog.sample(s, discardRatio); err != nil {
		return err
	}
	if err = vlog.rewrite(lf); err != nil {
		return err
	}
	return nil
}

// run_GC
func (vlog *valueLog) runGC(discardRatio float64, head *utils.ValuePtr) error {
	select {
	case vlog.garbageCh <- struct{}{}:
		defer func() {
			// 一次只能同时运行一个GC
			<-vlog.garbageCh
		}()

		var err error
		files := vlog.pickLog(head)
		if len(files) == 0 {
			return utils.ErrNoRewrite
		}

		tried := make(map[uint32]bool)
		for _, lf := range files {
			if _, ok := tried[lf.FID]; ok {
				continue
			}
			tried[lf.FID] = true
			if err = vlog.doRunGc(lf, discardRatio); err == nil {
				return nil
			}
		}
		return err
	default:
		return utils.ErrRejected
	}
}
