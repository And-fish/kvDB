package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"kvdb/utils"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type LogFile struct {
	Lock sync.RWMutex
	FID  uint32
	size uint32
	file *MmapFile
}

// 打开vlog文件，加载到内存中
func (lf *LogFile) Open(opt *Options) error {
	var err error
	lf.Lock = sync.RWMutex{}
	lf.FID = uint32(opt.FID)
	lf.file, err = OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Panic2(nil, err)

	fileInfo, err := lf.file.Fd.Stat()
	if err != nil {
		return utils.WarpErr("Unable to run file.Stat", err)
	}
	sz := fileInfo.Size()
	utils.CondPanic(sz > math.MaxUint32, fmt.Errorf("file size: %d greater than %d", uint32(sz), uint32(math.MaxUint32)))
	lf.size = uint32(sz)

	return nil
}

// 初始化，就是重新加载一遍size到内存中
func (lf *LogFile) Init() error {
	stats, err := lf.file.Fd.Stat()
	if err != nil {
		return errors.Wrapf(err, "Unable to check stat for %q", lf.FileName())
	}

	fileSize := stats.Size()
	if fileSize == 0 {
		return nil
	}
	utils.CondPanic(fileSize > math.MaxUint32, fmt.Errorf("[LogFile.Init] sz > math.MaxUint32"))

	lf.size = uint32(fileSize)
	return nil
}

// 按照ValuePtr来读取value数据
func (lf *LogFile) Read(p *utils.ValuePtr) (buf []byte, err error) {
	offset := p.Offset
	fileSize := len(lf.file.Data)
	valueSize := p.Len
	lfSize := atomic.LoadUint32(&lf.size)

	// 判断是否越界
	if offset >= uint32(fileSize) || offset+valueSize > uint32(fileSize) || offset+valueSize > lfSize {
		err = io.EOF
	} else {
		buf, err = lf.file.Bytes(int(offset), int(valueSize))
	}

	return buf, err

}

// 获取fileName
func (lf *LogFile) FileName() string {
	return lf.file.Fd.Name()
}

// 获取os.File对象
func (lf *LogFile) FD() *os.File {
	return lf.file.Fd
}

// 同步flush到磁盘中	(在flush之前必须要先加锁，要记得调用之前在外界Write()中会加锁)
func (lf *LogFile) Sync() error {
	return lf.file.Sync()
}

// 结束写入，截断文件
func (lf *LogFile) DoneWriting(offset uint32) error {
	if err := lf.file.Sync(); err != nil { // flush到磁盘
		return errors.Wrapf(err, "Unable to sync value log: %q", lf.FileName())
	}

	lf.Lock.Lock()
	defer lf.Lock.Unlock()

	// 截断文件
	if err := lf.file.Truncature(int64(offset)); err != nil {
		return errors.Wrapf(err, "Unable to truncate file: %q", lf.FileName())
	}
	if err := lf.Init(); err != nil { // 重载size
		return err
	}
	return nil
}

// 在offset后面写入一个buf []byte
func (lf *LogFile) Write(offset uint32, buf []byte) error {
	return lf.file.AppendBuffer(offset, buf)
}

// 截断操作的基本封装
func (lf *LogFile) Truncate(offset int64) error {
	return lf.file.Truncature(offset)
}

// 关闭
func (lf *LogFile) Close() error {
	return lf.file.Close()
}

// 获取size，原子操作
func (lf *LogFile) Size() int64 {
	return int64(atomic.LoadUint32(&lf.size))
}

// 原子的设置logfile.size，返回添加后的值
func (lf *LogFile) SetSize(size uint32) {
	atomic.StoreUint32(&lf.size, size)
}

// 跳转，从哪开始读取数据
func (lf *LogFile) Seek(offset int64, start int) (res int64, err error) {
	return lf.file.Fd.Seek(offset, start)
}

// encodeEntry，将Entry加码到buf中
func (lf *LogFile) EncodeEntry(entry *utils.Entry, buf *bytes.Buffer, offset uint32) (int, error) {
	h := utils.Header{
		KLen: uint32(len(entry.Key)),
		VLen: uint32(len(entry.Value)),
		TTL:  entry.TTL,
		Meta: entry.Meta,
	}

	// 先 --> 后:
	// 		+------------------------------------------------+
	//		|          header          | key | value | crc32 |
	// 		| meta | klen | vlen | ttl |          			 |
	// 		+------------------------------------------------+
	hash := crc32.New(utils.CastagnoliCrcTable)
	writers := io.MultiWriter(buf, hash)

	var headerEnc [utils.MaxHeaderSize]byte
	encSize := h.Encode(headerEnc[:])
	// multiWrter.Write会将buf写入到 multiWrter中所有的Wrter
	utils.Panic2(writers.Write(headerEnc[:encSize])) // 向writes中写入数据enc编码
	utils.Panic2(writers.Write(entry.Key))           // 写入key
	utils.Panic2(writers.Write(entry.Value))         // 写入value

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32()) // 将crc编码解析到crcBuf中
	utils.Panic2(buf.Write(crcBuf[:]))                  // 再把crcBuf写入到buf中
	return len(headerEnc[:encSize]) + len(entry.Key) + len(entry.Value) + len(crcBuf), nil
}

// 解码，将buf中的数据解码为Entry
func (lf *LogFile) DecodeEntry(buf []byte, offset uint32) (*utils.Entry, error) {
	var h utils.Header
	hlen := h.Decoder(buf) // 将固定大小的header解码出来
	kvc := buf[hlen:]
	entry := &utils.Entry{
		Meta:   h.Meta,
		TTL:    h.TTL,
		Offset: offset,
		Key:    kvc[:h.KLen],
		Value:  kvc[h.KLen : h.KLen+h.VLen],
	}
	return entry, nil
}
