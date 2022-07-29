package lsmt

import (
	"errors"
	"fmt"
	"kvdb/utils"
	"math"
	"unsafe"
)

type Options struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int

	DiscardStatsCh *chan map[uint32]int64
}

type header struct {
	overlap uint16
	diff    uint16
}

type tableBuilder struct {
	sstSzie       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	basekey       []byte
	staleDataSize int
	estimateSize  int64
}

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

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// 解码header
func (h *header) decode(buf []byte) {
	// 用一个haederSize数组重构h，再将buf的前headerSize赋值到这个数组，
	// 实际上就是用buf[:2] 和 buf[2:4] 对h两个元素的赋值
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

// 编码header
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

// 计算checksum
func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checksum := utils.CalculateChecksum(data)
	return utils.Uint64ToBytes(checksum)
}

// 为可能需要的size分配足够大的空间
func (tb *tableBuilder) allocate(size int) []byte {
	block := tb.curBlock
	// 如果剩下的空间小于需要的size
	if len(block.data[block.end:]) < size {
		// 如果扩两倍还不够就扩到需要的大小
		sz := 2 * len(block.data)
		if block.end+size > sz {
			sz = block.end + size
		}
		buf := make([]byte, sz)
		copy(buf, block.data)
		block.data = buf
	}
	block.end += size
	return block.data[block.end-size : block.end]
}

// 向curBlock.data中添加数据
func (tb *tableBuilder) append(data []byte) {
	// 在curBlock.data中请求一块空间
	b := tb.allocate(len(data))
	// 将data赋值到这块空间上
	utils.CondPanic(len(data) != copy(b, data), errors.New("tableBuilder.append data"))
}

// 结束bulider向block写入数据
func (tb *tableBuilder) finishBlock() {
	// 还没有开始
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// 将entryOffset和对应的长度写到curblock.data中，因为磁盘是一维结构
	tb.append(utils.Uint32Slice2Bytes(tb.curBlock.entryOffsets))
	tb.append(utils.Uint32ToBytes(uint32((len(tb.curBlock.entryOffsets)))))

	// 计算block的checksum
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	/*
		block 外 -> 内
		读取时会从外到内一层一层读取
		+---------------------------------------------------------------------+
		| checksum_len | checksum | entryOffset_len | entryOffset | Key-Value |
		+---------------------------------------------------------------------+
	*/
	tb.append(checksum)
	tb.append(utils.Uint32ToBytes(uint32((len(checksum)))))
	//
	tb.estimateSize += tb.curBlock.estimateSize
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
	return
}

// 检查是否block是否已经写满了，是否需要像新的block中写入
func (tb *tableBuilder) tryFinishBlock(entry *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	// 如果condition为true，会panic
	utils.CondPanic((!(uint32(len(tb.curBlock.entryOffsets)+1)*4+4+4 < math.MaxUint32)), errors.New("Integer overflow"))

	entriesOffsetSize := int64((len(tb.curBlock.entryOffsets)+1)*4 + // entryOffset是[]uint32，加上即将存入的一个再乘4
		4 + // entryOffset_len是uint32，占4字节
		8 + // uint64转为[]byte占8字节
		4) // hecksum_len是uint32，占4字节

	tb.curBlock.estimateSize = int64(tb.curBlock.end) + //之前的长度
		int64(6) + // entry的header
		int64(len(entry.Key)) + // entry.key
		int64(entry.EntryEncodedSize()) + // entry.meta + entry.value +entry.ttl
		entriesOffsetSize // blcok.data之前的长度

	utils.CondPanic(!(uint64(tb.curBlock.estimateSize)+uint64(tb.curBlock.end) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSize > int64(tb.opt.BlockSize)
}

// 查找key和preKey不同的部分
func (tb *tableBuilder) getDiffKey(newkey []byte) []byte {
	var i int
	// 依次对比找到相同的部分
	for i = 0; i < len(newkey) && i < len(tb.curBlock.baseKey); i++ {
		// 如果发现某个不一样就break
		if newkey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
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
	if tb.tryFinishBlock(entry) {
		if isStable {
			tb.staleDataSize += len(key) + 4
		}
		tb.finishBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTimeStamp(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.getDiffKey(key)
	}

	// 判断相同前缀的长度是不是能用uint16表示，判断不同的部分长度能不能用uint16表示
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	// 为value申请足够大的编码空间
	valueBuf := tb.allocate(int(value.ValEncodedSize()))
	// 将value编码到valueBuf上
	value.ValEncoding(valueBuf)
}
func (tb *tableBuilder) buildIndex(filter []byte) {

}

// func (tb *tableBuilder) done() buildData {
// 	tb.finishBlock()
// 	if len(tb.blockList) == 0 {
// 		return buildData{}
// 	}
// 	buildData := buildData{
// 		blockList: tb.blockList,
// 	}

// 	// 创建bllomFilter
// 	var filter utils.Filter
// 	if tb.opt.BloomFalsePositive > 0 {
// 		// 根据optine的假阳率和key的个数计算bitsperkey
// 		bitsperkey := utils.BitsPerkey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
// 		filter = utils.NewFilter(tb.keyHashes, bitsperkey)
// 	}

// 	index, dataSize := tb.buildIndex()
// }

// func (tb *tableBuilder) finish() []byte {

// }

// // 将Entry添加到Builder中
// func (tb *tableBuilder) add(e *utils.Entry, isStable bool) {
// 	key := e.Key
// 	val := utils.ValueStruct{
// 		Meta:  e.Meta,
// 		Value: e.Value,
// 		TTL:   e.TTL,
// 	}
// 	if tb.
// }
