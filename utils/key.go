package utils

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"
)

type stringStruct struct {
	str    unsafe.Pointer
	lenght int
}

func ParseKey(sourceKey []byte) (realKey []byte) {
	if len(sourceKey) <= 8 {
		realKey = sourceKey
		return
	}
	realKey = sourceKey[:len(sourceKey)-8]
	return
}
func ParseTimeStamp(sourceKey []byte) (timestamp uint64) {
	if len(sourceKey) <= 8 {
		timestamp = 0
		return
	}
	timestamp = math.MaxUint64 - binary.BigEndian.Uint64(sourceKey[len(sourceKey)-8:])
	return
}
func IsSameKey(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}
	return bytes.Equal(ParseKey(key1), ParseKey(key2))
}

// 为key添加上TimeStamp
func KeyWithTS(key []byte, ts uint64) []byte {
	res := make([]byte, len(key)+8)
	copy(res, key)
	binary.BigEndian.PutUint64(res[len(key):], math.MaxUint64-ts)
	return res
}

// copy
func SafeCopy(needKey, key []byte) []byte {
	return append(needKey[:0], key...)
}
