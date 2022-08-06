package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"kvdb/utils"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type WalFile struct {
	lock    *sync.RWMutex
	file    *MmapFile
	opt     *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

// Fid
func (wf *WalFile) GetFid() uint64 {
	return wf.opt.FID
}

// Close()
func (wf *WalFile) Cloce() error {
	fileName := wf.file.Fd.Name()
	if err := wf.file.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

// Name
func (wf *WalFile) Name() string {
	return wf.file.Fd.Name()
}

// Size,返回已经被写入数据的size
func (wf *WalFile) GetSize() uint32 {
	return wf.writeAt
}

// 打开WalFile文件
func OpenWalFile(opt *Options) *WalFile {
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{
		lock: &sync.RWMutex{},
		file: mf,
		opt:  opt,
		buf:  &bytes.Buffer{},
		size: uint32(len(mf.Data)),
	}
	utils.Err(err)
	return wf
}

// 向Wal中写入entry
func (wf *WalFile) Write(entry *utils.Entry) error {
	wf.lock.Lock()

	walLen := utils.WalCodec(wf.buf, entry) // 先写入到buf中
	buf := wf.buf.Bytes()
	utils.Panic(wf.file.AppendBuffer(wf.writeAt, buf)) // 再写入到mmap中
	wf.writeAt += uint32(walLen)

	wf.lock.Unlock()
	return nil
}

// 截断wal文件
func (wf *WalFile) Truncate(size int64) error {
	if size <= 0 {
		return nil
	}
	if fileInfo, err := wf.file.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", wf.Name(), err)
	} else if fileInfo.Size() == size {
		return nil
	}

	wf.size = uint32(size)
	return wf.file.Truncature(size)
}

// KV分离
type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *WalFile
}

func (sr *SafeRead) MakeEntry(reader io.Reader) (*utils.Entry, error) {
	hr := utils.NewHashReader(reader)
	var wh utils.WalHander
	hlen, err := wh.Decode(hr)
	if err != nil {
		return nil, err
	}
	if wh.KeyLen > uint32(1<<16) { // key_len要小于uint16
		return nil, utils.ErrTruncate
	}
	keyLen := wh.KeyLen
	if cap(sr.K) < int(keyLen) {
		sr.K = make([]byte, 2*keyLen)
	}
	valLen := wh.ValueLen
	if cap(sr.V) < int(valLen) {
		sr.V = make([]byte, 2*valLen)
	}

	// MakeEntry
	entry := &utils.Entry{
		Offset: sr.RecordOffset,
		Hlen:   hlen,
	}
	buf := make([]byte, wh.KeyLen+wh.ValueLen)
	if _, err := io.ReadFull(hr, buf[:]); err != nil { // 不能读到完整的buf会报ErrTruncate
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	entry.Key = buf[:wh.KeyLen]
	entry.Value = buf[wh.KeyLen:]

	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil { // 不能读到完整的crcBuf会报ErrTruncate
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.Bytes2Uint32(crcBuf[:])
	if crc != hr.Sum32() { // 校验
		return nil, utils.ErrTruncate
	}

	entry.TTL = wh.TTL
	return entry, nil
}

// 从磁盘中遍历获取wal数据
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(wf.file.NewReader(int(offset)))
	sr := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           wf,
	}
	var validEndOffset = uint32(offset)

loop:
	for {
		entry, err := sr.MakeEntry(reader)
		switch {
		case err == io.EOF: // 读完了就跳出
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate: // 读取结构化数据发生EOF
			break loop
		case err != nil:
			return 0, err
		case entry.IsZero():
			break loop
		}

		var vp utils.ValuePtr
		size := uint32(entry.LogHeaderLen() + len(entry.Key) + len(entry.Value) + crc32.Size)
		sr.RecordOffset += size
		validEndOffset = sr.RecordOffset
		if err := fn(entry, &vp); err != nil { //对entry的一些处理函数
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}

	return validEndOffset, nil
}
