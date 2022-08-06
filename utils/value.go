package utils

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"
)

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

// size of vlog header.
// +----------------+------------------+
// | keyID(8 bytes) |  baseIV(12 bytes)|
// +----------------+------------------+
const ValueLogHeaderSize = 20
const vptrSize = unsafe.Sizeof(ValuePtr{})

// 判断两个ValuePtr的新旧，如果 p < o，返回true
func (p ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	// fid大的vp更新
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	// offset大的vp更新
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	// len大的vp更新
	return p.Len < o.Len
}

// 判断ValuePtr是否为0值
func (p ValuePtr) IsZero() bool {
	return p.Len == 0 && p.Offset == 0 && p.Fid == 0
}

// 将ValuePtr编码为[]byte
func (p ValuePtr) Encode() []byte {
	buf := make([]byte, vptrSize)
	*(*ValuePtr)(unsafe.Pointer(&buf[0])) = p
	return buf
}

// 将[]byte解码为ValuePtr
func (p *ValuePtr) Decode(buf []byte) {
	// 首先将valuePtr转化为byte数组，再将buf赋值过来
	copy((*[vptrSize]byte)(unsafe.Pointer(p))[:], buf[:vptrSize])
}

// 判断
func IsValuePtr(e *Entry) bool {
	// 校验，Set if the value is NOT stored directly next to key.
	return e.Meta&BitValuePointer > 0
}

// 将byte数组转化为uint32，直接读取
// 例如buf == [139,39] == [10001000,00100111]，
// 使用binary.Uvarint(buf)可以读取到5000，0010011 10001000
// 使用binary.BigEndian.Uint32(buf)会不考虑编码规则，按照大端读取32位，可以读取到2284257280，10001000 00100111 0000000000000000
// 使用binary.LittleEndian.Uint32(buf)会不考虑编码规则，按照小端读取32位，可以读取到10120，0000000000000000 00100111 10001000 0000000000000000
func Bytes2Uint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// 将byte数组转化为uint64，直接读取
func Bytes2Uint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// 将uint32转化为byte数组
func Uint32ToBytes(u32 uint32) []byte {
	var buf [U32Size]byte
	binary.BigEndian.PutUint32(buf[:], u32)
	return buf[:]
}

// 将uint64转化为byte数组
func Uint64ToBytes(u64 uint64) []byte {
	var buf [U64Size]byte
	binary.BigEndian.PutUint64(buf[:], u64)
	return buf[:]
}

// 将uint32切片转化为byte数组
func Uint32Slice2Bytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var buf []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	header.Len = len(u32s) * 4 // 一个uint32占4byte
	header.Cap = header.Len
	header.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return buf
}

// 将byte数组转化为uint32切片
func Bytes2Uint32Slice(buf []byte) []uint32 {
	if len(buf) == 0 {
		return nil
	}
	var u32s []uint32
	header := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	header.Len = len(buf) / 4 // 四个byte能转化为一个uint32
	header.Cap = header.Len
	header.Data = uintptr(unsafe.Pointer(&buf[0]))
	return u32s
}

//////

// ValuePtrCodec _
func ValuePtrCodec(vp *ValuePtr) []byte {
	return []byte{}
}

// RunCallback _
func RunCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func DiscardEntry(e, vs *Entry) bool {
	// TODO 版本这个信息应该被弱化掉 在后面上MVCC或者多版本查询的时候再考虑
	// if vs.Version != ParseTs(e.Key) {
	// 	// Version not found. Discard.
	// 	return true
	// }
	if IsDeletedOrExpired(vs.Meta, vs.TTL) {
		return true
	}
	if (vs.Meta & BitValuePointer) == 0 {
		// Key also stores the value in LSM. Discard.
		return true
	}
	return false
}
