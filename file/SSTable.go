package file

import (
	"io"
	"kvdb/pb"
	"kvdb/utils"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

/*
	解码就是从外到内层层解封装
	SSTable整体的结构：外 ---> 内
	+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	| checksum_len | checksum | blockIndex_len | blockIndex_len |                                                      BlcokData1                                       |
	|                                                           | checksum_len | checksum | entryIndex_len | entruIndex |			       entry(k-v)1                  |
	|																													| value | ttl | meta | diffKey | diff | overlap |
	+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
*/

// SSTable在内存中的表现形式
type SSTable struct {
	lock           *sync.RWMutex
	file           *MmapFile
	maxKey         []byte
	minKey         []byte
	idxTable       *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}

// 将SSTable文件读取到内存中。封装的是mmapfile的openMmapFile()
func OpenSSTable(opt *Options) *SSTable {
	// 读写权限 + 如果文件不存在就创建
	mf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{
		file: mf,
		fid:  opt.FID, // 唯一FID
		lock: &sync.RWMutex{},
	}
}

// 对data的抽象的read，根据offset和size可以定制化
func (sst *SSTable) read(offset, size int) ([]byte, error) {
	if len(sst.file.Data) > 0 {
		if len(sst.file.Data[offset:]) < size {
			// 如果剩下的data不足以读取需要的size，返回错误
			return nil, io.EOF
		}
		return sst.file.Data[offset : offset+size], nil
	}

	res := make([]byte, size)
	// 从offset开始读取size的字节到res上
	_, err := sst.file.Fd.ReadAt(res, int64(offset))
	return res, err
}

// 读取checksumOffset
func (sst *SSTable) readCheckError(offset, size int) []byte {
	buf, err := sst.read(offset, size)
	utils.Panic(err)
	return buf
}

// 初始化SSTable
func (sst *SSTable) initSSTable() (BOffset *pb.BlockOffset, err error) {
	dataSize := len(sst.file.Data)
	dataSize -= 4
	// 读取最后四个byte作为Checksum_len
	buf := sst.readCheckError(dataSize, 4)
	checksum_len := utils.Bytes2Uint32(buf)
	if checksum_len < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// 通过checksum_len读取checksum
	dataSize -= int(checksum_len)
	checksum := sst.readCheckError(dataSize, int(checksum_len))

	// 再往后读4byte，读出blockIndex_len
	dataSize -= 4
	buf = sst.readCheckError(dataSize, 4)
	sst.idxLen = int(utils.Bytes2Uint32(buf))

	// 后面就是blockIndex(和一些其他的)
	dataSize -= sst.idxLen
	sst.idxStart = dataSize
	data := sst.readCheckError(dataSize, sst.idxLen)

	// 校验checksum
	if err := utils.VerifyChecksum(data, checksum); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", sst.file.Fd.Name())
	}
	// 将data Unmarshal为TableIndex结构
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	sst.idxTable = indexTable

	// 判断是否有bloomFilter
	sst.hasBloomFilter = (len(indexTable.BloomFilter) > 0)
	// SSTIndex中是否有blockIndex
	if len(indexTable.GetOffsets()) > 0 {
		// 返回第一个
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

// 初始化
func (sst *SSTable) Init() error {
	var BOffset *pb.BlockOffset
	var err error
	// 初始化sstable，并获取到sstable的第一个BlockOffset
	if BOffset, err = sst.initSSTable(); err != nil {
		return err
	}
	// 为sstable获取创建时间
	stat, _ := sst.file.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	// Atim：访问时间 Access time；
	// Mtim：修改时间 Modify time；
	// Ctim：创建时间 Create Time
	sst.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)

	// 最小的key，第一个Block的第一个key(baseKey)
	keyBytes := BOffset.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	sst.minKey = minKey
	sst.maxKey = minKey

	return nil
}

// 封装一下给sstable设置maxKey，也就是最后一个block的最后一个key
func (sst *SSTable) SetMaxKey(maxKey []byte) {
	sst.maxKey = maxKey
}

// Close 关闭
func (sst *SSTable) Close() error {
	return sst.file.Close()
}

// Indexs
func (sst *SSTable) GetIndexs() *pb.TableIndex {
	return sst.idxTable
}

// MaxKey
func (sst *SSTable) GetMaxKey() []byte {
	return sst.maxKey
}

// MinKey
func (sst *SSTable) GetMinKey() []byte {
	return sst.minKey
}

// FID
func (sst *SSTable) GetFID() uint64 {
	return sst.fid
}

// HasBloomFilter
func (sst *SSTable) HasBloomFilter() bool {
	return sst.hasBloomFilter
}

// 返回sstable.file.data从offset开始size的[]byte
func (sst *SSTable) Btyes(offset, size int) ([]byte, error) {
	// 从mmapfile.data中读取
	return sst.file.Bytes(offset, size)
}

// 返回文件的大小
func (sst *SSTable) Size() int64 {
	stat, err := sst.file.Fd.Stat()
	utils.Panic(err)
	return stat.Size()
}

// 获得sstable创建时间
func (sst *SSTable) GetCreatedAt() *time.Time {
	return &sst.createdAt
}

// 设置sstable创建时间 、
func (sst *SSTable) SetCreatedAt(time *time.Time) {
	sst.createdAt = *time
}

// 封装了Mmapfile的Delete()
func (sst *SSTable) Delete() error {
	return sst.file.Delete()
}

// 将mmap强行Truncature到size
func (sst *SSTable) Truncature(size int64) error {
	return sst.file.Truncature(size)
}
