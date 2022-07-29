package utils

import (
	"encoding/binary"
	"time"
)

// 最外层的写入结构所以拥有所有的数据
type Entry struct {
	/*
		前  --->  后
		+------------------------------------+
		| realKey | timestamp(uint64,8bytes) |
 		+------------------------------------+
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

// 计算ValueStruct中TTL的大小
func GetIntSize(intval uint64) int {
	size := 1
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
	buf[0] = v.Meta
	ttlsize := binary.PutUvarint(buf, v.TTL)
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
