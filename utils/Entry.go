package utils

import (
	"encoding/binary"
	"time"
)

// 最外层的写入结构所以拥有所有的数据
type Entry struct {
	/*
				Key 前  --->  后
				+---------------------------------------+
				| realKey : uint64 | timestamp : 8bytes |
		 		+---------------------------------------+
	*/
	Key   []byte
	Value []byte
	TTL   uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int //Lenght of the header
	ValThreshold int64
}

// Value结构体
type ValueStruct struct {
	Meta    byte
	Value   []byte
	TTL     uint64
	Version uint64
}

// 计算int编码需要的长度 (计算ValueStruct中TTL的大小)
func GetIntSize(intval uint64) int {
	/*
		110111 11000100 10011110
		在编码过程中会每8位(1Byte)会有一个标识位，7个数据位
		所以1Btye有效位是7位
		binary.PutUvarint()		按照小端编码
		所以上面的int编码后的大小 ==> 4Byte	==> [158 137 223 1]	==> [10011110 10001001 11011111 1]
		标识为是每个byte的最高位，如果标识位为1表示当前还没有结束
	*/
	size := 0
	for {
		size++
		intval >>= 7
		if intval == 0 {
			break
		}
	}
	return size
}

// 计算Value编码长度，长度为meta + value + TTL
func (v *ValueStruct) ValEncodedSize() uint32 {
	valSize := len(v.Value) + 1
	ttlSize := GetIntSize(v.TTL)
	return uint32(valSize + ttlSize)
}

// 将Value编码到传入的buf上，返回大小
func (v *ValueStruct) ValEncoding(buf []byte) uint32 {
	/*
		[Meta + TTL + Value]
	*/
	buf[0] = v.Meta
	ttlsize := binary.PutUvarint(buf[1:], v.TTL)
	n := copy(buf[1+ttlsize:], v.Value)
	return uint32(n + ttlsize + 1)
}

// 将value所在的buf解码
func (v *ValueStruct) ValDecode(buf []byte) {
	v.Meta = buf[0]
	size := int(0)
	v.TTL, size = binary.Uvarint(buf[1:])
	v.Value = buf[size+1:]
}

// 使得Entry结构满足Item接口
func (e *Entry) Entry() *Entry {
	return e
}

// 根据传入的key和vaue初始化创建entry
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

// 判断entry是否失效，如果失效了返回false
func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
		// 如果value == nil认为entry被删除
		return true
	}
	if e.Entry().TTL == 0 {
		// 如果为0，表示没有设置过期时间，也就不会过期
		return false
	}
	// 让判断当前entry的ttl是否过期
	return e.TTL <= uint64(time.Now().Unix())
}

// 设置过期时间
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.TTL = uint64(time.Now().Add(dur).Unix())
	return e
}

// 计算Entry编码的Size
func (e *Entry) EntryEncodedSize() uint32 {
	valueSize := len(e.Value)
	ttlSize := GetIntSize(e.TTL)
	ttlSize += GetIntSize(uint64(e.Meta))
	return uint32(valueSize) + uint32(ttlSize)
}

// 预估大小，用于判断是否 存在/应该放在 于Vlog中，(不考虑checksum等元信息的大小)
func (e *Entry) EstimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1 //
	}
	return len(e.Key) + 12 + 1 // key + valuePtr + mate
}

// 如果value要放到valuelog中，需要在sst中添加的是header而不是value本身
type Header struct {
	KLen uint32
	VLen uint32
	TTL  uint64
	Meta byte
}

// 将header编码到buf中，并返回编码长度
/*
	HeaderBuf ：0 --> last
	+--------------------------+
	| meta | KLen | VLen | TTL |
	+--------------------------+
*/
func (h Header) Encode(buf []byte) int {
	buf[0] = h.Meta
	index := 1
	index += binary.PutUvarint(buf[index:], uint64(h.KLen))
	index += binary.PutUvarint(buf[index:], uint64(h.VLen))
	index += binary.PutUvarint(buf[index:], h.TTL)
	return index
}

// 对buf解码为Header
func (h *Header) Decoder(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	index += count
	ttl, count := binary.Uvarint(buf[index:])
	index += count

	h.KLen = uint32(klen)
	h.VLen = uint32(vlen)
	h.TTL = ttl
	return index
}

// 对HashReader reader解码为header
func (h *Header) DecodeFrom(reader *HashReader) (int, error) {

	var err error
	h.Meta, err = reader.ReadByte() // 第一个byte是meta
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader) // 读一个uint64
	if err != nil {
		return 0, err
	}
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.TTL, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	h.VLen = uint32(vlen)
	return reader.BytesRead, nil

}
