package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"kvdb/utils/mmap"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const oneGB = 1 << 30

// 用于表示一个通过mmap映射的文件
type MmapFile struct {
	// 实际放置数据的[]byte
	Data []byte
	// File唯一标识
	Fd *os.File // File是文件描述符
	/*
		type file struct {
		    pfd         poll.FD
		    name        string
		    dirinfo     *dirInfo // nil unless directory being read
		    nonblock    bool     // whether we set nonblocking mode
		    stdoutOrErr bool     // whether this is stdout or stderr
		    appendMode  bool     // whether file is opened for appending
		}
	*/
}

// 用mmap将文件映射到内存中，返回MmapFile
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// 如果file是空的(filesize == 0)
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	// fmt.Printf("Mmaping file: %s with writable: %v filesize: %d\n", fd.Name(), writable, fileSize)
	buf, err := mmap.Mmap(fd, writable, fileSize) // 通过mmap设置映射
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

// 将一个文件按照Mmap的方式打开。(会调用OpenMmapFileUsing()) 返回MmapFile的格式
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// fmt.Printf("opening file %s with flag: %v\n", filename, flag)
	// -rw-rw-rw-
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	// 如果传入的flag是O_RDONLY，则 writable = false
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	// 如果 sst文件被打开过，则使用其文件原来的大小
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return OpenMmapFileUsing(fd, maxSz, writable)
}

// Reader
type mmapReader struct {
	Data   []byte
	offset int
}

// 实现Reader接口,Read
func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	// 将Data[offset:]复制到buf中
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// 从offset开始读取Data中size个byte
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// Truncature 截断
// 因为mmap分配的是page size的倍数，为了缓解空间浪费，需要截断
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil { // 先将内存中的数据写回到磁盘中
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil { // 文件描述符页需要修改
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	// 再重新分配Data
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

// 从offset开始读取一段silce
func (m *MmapFile) Slice(offset int) []byte {
	// AllocateSlice()中将前四个byte保存了size的大小
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

// 将MmapFile.Data扩大size
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// 如果MmapFile.data太小，就扩大一倍；如果扩大一倍太多了，就扩大1GB；如果还不够就扩大需要的size
	if start+sz > len(m.Data) {
		growBy := len(m.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}
	// 前4bytes是size的编码
	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

// AppendBuffer 向内存中写入一个buf，如果空间不足则重新映射，扩大空间
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	// 如果空间不足
	if end > size {
		growBy := size
		// 如果需要扩大一倍太多了，就扩大1GB
		if growBy > oneGB {
			growBy = oneGB
		}
		// 如果扩大的不够，就扩大需要的size
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	// 将buf复制到mmap.data中
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// 将内存中的MmapFile写回到磁盘中
func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

// 删除文件
func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}
	// 取消映射
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	// 修改文件描述符的size为0
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	// close file
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	// 删除指定的文件
	return os.Remove(m.Fd.Name())
}

// Close流程
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	// 写回到磁盘
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	// 取消映射
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	// close file
	return m.Fd.Close()
}

// 写入目录
func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	// 同步dir
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	// 关闭dir
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

// ReName 兼容接口
// TODO
func (m *MmapFile) ReName(name string) error {
	return nil
}
