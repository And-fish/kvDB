package lsmt

import (
	"errors"
	"fmt"
	"kvdb/file"
	"kvdb/pb"
	"kvdb/utils"
	"math"
	"os"
	"unsafe"
)

// entry的上层，表示key的相同/不同部分的长度
type header struct {
	overlap uint16
	diff    uint16
}

// tableBuilder用于存储构建sstable的信息
type tableBuilder struct {
	sstSzie       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32 // 是builder中所有的key的hash值
	maxVersion    uint64
	basekey       []byte
	staleDataSize int
	estimateSize  int64
}

// 每个block的，用于存储entry
type block struct {
	offset            int
	checksum          []byte
	entriesIndexStart int
	checkLen          int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSize      int64
}

// 由tableBuilder转化得到，里面的字段直接与构建sstable相关
type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

// header的大小为4
const headerSize = uint16(unsafe.Sizeof(header{}))

func (b *block) verifyChecksum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

// 解码header，将[]byte转化为header
func (h *header) decode(buf []byte) {
	// 用一个haederSize数组重构h，再将buf的前headerSize赋值到这个数组，
	// 实际上就是用buf[:2] 和 buf[2:4] 对h两个元素的赋值
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

// 编码header，将header转化为[]byte
func (h header) encode() []byte {
	var buf [headerSize]byte
	// 将buf按照header重构，再赋值
	*(*header)(unsafe.Pointer(&buf[0])) = h
	return buf[:]
}

// 根据option初始化创建一个Builder
func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSzie: opt.SSTableMaxSz,
	}
}

// 根据option和指定的size初始化创建一个Builder
func newTableBuilderWithSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSzie: size,
	}
}

// 判断builder是否为空，为空返回true
func (tb *tableBuilder) isEmpty() bool {
	return len(tb.keyHashes) == 0
}

// 计算checksum，将checksum转化为[]byte
func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checksum := utils.CalculateChecksum(data)
	return utils.Uint64ToBytes(checksum)
}

// 为可能需要的size分配足够大的空间，并返回分配的空间
func (tb *tableBuilder) allocate(size int) []byte {
	block := tb.curBlock
	// 如果剩下的空间小于需要的size
	if len(block.data[block.end:]) < size {
		// 如果扩两倍还不够就扩到需要的大小
		sz := 2 * len(block.data)
		if block.end+size > sz {
			sz = block.end + size
		}
		// 创建足够大的[]byte
		buf := make([]byte, sz)
		// 将olddata拷贝到新建的buf上，再将block对应的data换成新的buf
		copy(buf, block.data)
		block.data = buf
	}
	// 将记录block写入到的endOffset加上这次要写入的size
	block.end += size
	// 返回分配好的待写入的[]byte
	return block.data[block.end-size : block.end]
}

// 向curBlock.data中添加数据
func (tb *tableBuilder) append(data []byte) {
	// 在curBlock.data中请求对应大小的空间
	b := tb.allocate(len(data))
	// 将data赋值到这块空间上
	utils.CondPanic(len(data) != copy(b, data), errors.New("tableBuilder.append data"))
}

// 结束bulider向block写入数据，也就是开始封装block
func (tb *tableBuilder) finishBlock() {
	// 如果还没有开始，直接返回
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// 将entryOffset和对应的长度写到curblock.data中，因为磁盘是一维结构
	tb.append(utils.Uint32Slice2Bytes(tb.curBlock.entryOffsets))
	tb.append(utils.Uint32ToBytes(uint32((len(tb.curBlock.entryOffsets)))))

	// 计算block的checksum
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	/*
		block 外 -> 内											    Key-Value在add()中添加，finishBlock()只是封装block的entry的元数据
		读取时会从外到内一层一层读取
		+---------------------------------------------------------------------+
		| checksum_len | checksum | entryOffset_len | entryOffset | Key-Value |
		+---------------------------------------------------------------------+
	*/
	tb.append(checksum)
	tb.append(utils.Uint32ToBytes(uint32((len(checksum)))))

	// 将tableBuilder的大致size 加上curBlock的大致Size
	tb.estimateSize += tb.curBlock.estimateSize
	// 在tableBuild中写过的BlockList中加上curBlock
	tb.blockList = append(tb.blockList, tb.curBlock)
	// 将tableBuilder的key计数加上entry的个数
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))

	tb.curBlock = nil
	return
}

// 检查是否block是否已经写满了，是否需要开辟一个新的block写入
func (tb *tableBuilder) tryFinishBlock(entry *utils.Entry) bool {
	// 如果当前没有block，需要开辟
	if tb.curBlock == nil {
		return true
	}
	// 如果当前的block还没开始写入，不需要
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	// 如果condition为true，会panic
	// 判断已经写入的entry + 待写的entry *4 + 4 + 4，是否越界了
	utils.CondPanic((!(uint32(len(tb.curBlock.entryOffsets)+1)*4+4+8+4 < math.MaxUint32)), errors.New("Integer overflow"))

	entriesOffsetSize := int64((len(tb.curBlock.entryOffsets)+1)*4 + // entryOffset是[]uint32，加上即将存入的一个再乘4
		4 + // entryOffset_len是uint32，占4字节
		8 + // uint64转为[]byte占8字节
		4) // checksum_len是uint32，占4字节

	tb.curBlock.estimateSize = //大致大小
		int64(tb.curBlock.end) + //之前的长度
			int64(6) + // entry的header
			int64(len(entry.Key)) + // entry.key
			int64(entry.EntryEncodedSize()) + // entry.meta + entry.value +entry.ttl
			entriesOffsetSize // blcok.data之前的长度

	utils.CondPanic(!(uint64(tb.curBlock.estimateSize)+uint64(tb.curBlock.end) < math.MaxUint32), errors.New("Integer overflow"))

	// 判断大致的大小是否超过了option的大小
	return tb.curBlock.estimateSize > int64(tb.opt.BlockSize)
}

// 查找key和baseKey不同的部分
func (tb *tableBuilder) getDiffKey(newkey []byte) []byte {
	var i int
	// 依次对比找到相同的部分
	for i = 0; i < len(newkey) && i < len(tb.curBlock.baseKey); i++ {
		// 如果发现某个不一样就break
		if newkey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	// 返回diffKey
	return newkey[i:]
}

// 向block.data写入Key-Value(Entry)
func (tb *tableBuilder) add(entry *utils.Entry, isStable bool) {
	key := entry.Key
	value := utils.ValueStruct{
		Meta:  entry.Meta,
		Value: entry.Value,
		TTL:   entry.TTL,
	}
	// 检查当前block是否写满了
	if tb.tryFinishBlock(entry) {
		if isStable {
			tb.staleDataSize += len(key) + 4
		}
		// 如果写满了就将前一个block封装
		tb.finishBlock()
		// 创建一个新的block作为curblock
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}
	// 向tableBuilder记录keyHash的[]uint32中添加enrt的key
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	// 更新最新版本号
	if version := utils.ParseTimeStamp(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	// 获取diffKey
	var diffKey []byte
	// 如果当前没有baseKey，那么说明当前entry是curBlock的第一个entry，将这个entry的key作为baseKey
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		// 否则就去get一下
		diffKey = tb.getDiffKey(key)
	}

	// 判断相同前缀的长度是不是能用uint16表示，判断不同的部分长度能不能用uint16表示
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	// 在curBlock中记录entryOffset
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	// 向curBlock的Data部分添加header和diffkey
	tb.append(h.encode())
	tb.append(diffKey)

	// 为value申请足够大的编码空间
	valueBuf := tb.allocate(int(value.ValEncodedSize()))
	// 将value编码到valueBuf上
	value.ValEncoding(valueBuf)

	/*
		entry：外 ---> 内
		+-----------------------------------------------+
		| value | ttl | meta | diffkey | diff | overlap |
		+-----------------------------------------------+
	*/
}

// 添加旧的key，和压缩merge有关
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 + 4
	tb.add(e, true)
}

// 添加Key
func (tb *tableBuilder) Addkey(entry *utils.Entry) {
	tb.add(entry, false)
}

// 为单个的block创建索引，转化为BlockOffset
func (tb *tableBuilder) writeBlockOffset(b *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = b.baseKey
	offset.Len = uint32(b.end)
	offset.Offset = startOffset
	return offset
}

// 为ttableBuilder中所有的block创建索引
func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startoffset uint32
	var offsets []*pb.BlockOffset
	for _, b := range tb.blockList {
		offset := tb.writeBlockOffset(b, startoffset)
		offsets = append(offsets, offset)
		startoffset += offset.Len
	}
	return offsets
}

// 创建TableIndex(BlockIndexs)索引
func (tb *tableBuilder) buildIndex(filter []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(filter) > 0 {
		tableIndex.BloomFilter = filter
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	// 为tableBuilder中的所有block创建索引，并添加到tableIndex中
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)

	var dataSize uint32
	for i := range tb.blockList {
		// 将tableBuilder中所有block的数据data统计一下size
		dataSize += uint32(tb.blockList[i].end)
	}
	// 将
	data, err := tableIndex.Marshal()
	utils.Err(err)
	// 返回tableIndex序列化后的[]byte 和 所有block的size
	return data, dataSize
}

// 将tableBuilder转化为buildData供后续使用
func (tb *tableBuilder) done() buildData {
	// 结束向block写入数据，所有blcok.data此时都封装成功了EntryIndex
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	buildData := buildData{
		blockList: tb.blockList,
	}

	// 创建bllomFilter
	var filter utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		// 根据optine的假阳率和key的个数计算bitsperkey，并创建合适的bloomFilter
		bitsperkey := utils.BitsPerkey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		filter = utils.NewFilter(tb.keyHashes, bitsperkey) // 将builder中所有的key都插入到filter中
	}

	// 获取blockIndex和所有block.data的大小
	blockindex, dataSize := tb.buildIndex(filter)
	// 根据索引计算checksum
	checksum := tb.calculateChecksum(blockindex)
	buildData.index = blockindex
	buildData.checksum = checksum
	// 总大小为blockData + BlcokIndex + checksum_len + blockindex_len
	buildData.size = int(dataSize) + len(blockindex) + 4 + 4 + len(checksum)
	return buildData
}

// 将buildData写入到buf中
func (bd *buildData) Copy(buf []byte) (written int) {
	for _, block := range bd.blockList {
		written += copy(buf[written:], block.data[:block.end])
	}
	/*
		外 --> 内
		+-------------------------------------------------------------------+
		| ckecksum_len | checksum | BlockIndex_len | BlockIndex | BlockData |
		+-------------------------------------------------------------------+
	*/
	written += copy(buf[written:], bd.index)
	written += copy(buf[written:], utils.Uint32ToBytes(uint32(len(bd.index))))
	written += copy(buf[written:], bd.checksum)
	written += copy(buf[written:], utils.Uint32ToBytes(uint32(len(bd.checksum))))
	return written
}

// 将tableBuilder转化为buf []byte，事先总体的封装
func (tb *tableBuilder) finish() []byte {
	blockData := tb.done()
	buf := make([]byte, blockData.size)
	written := blockData.Copy(buf)
	utils.CondPanic(len(buf) == written, nil)
	return buf
}

/*
	SSTable整体的结构：外 ---> 内
	+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	| checksum_len | checksum | blockIndex_len | blockIndex_len |                                                      BlcokData1                                       |
	|                                                           | checksum_len | checksum | entryIndex_len | entruIndex |			       entry(k-v)1                  |
	|																													| value | ttl | meta | diffKey | diff | overlap |
	+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
*/

// 将tableBuilder生成table(将数据放到mmap [] byte中)返回对应的table
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (*table, error) {
	builddata := tb.done()
	table := &table{
		lm:  lm,
		fid: utils.FID(tableName),
	}

	table.sst = file.OpenSSTable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    builddata.size,
	})

	buf := make([]byte, builddata.size)
	// 将buildData写入到buf中，返回长度
	written := builddata.Copy(buf)
	utils.CondPanic((written != len(buf)), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	// 从sst.MmapFile.data读取一个足够大的空间
	sstBuf, err := table.sst.Btyes(0, builddata.size)
	if err != nil {
		return nil, err
	}
	// 将BuildData写入到MmapFile中
	copy(sstBuf, buf)
	return table, nil
}

// 判断是否满了
func (tb *tableBuilder) IsReachedCapacity() bool {
	return tb.estimateSize > tb.sstSzie
}

// Close
func (tb *tableBuilder) Close() {

}
