package utils

import "math"

const seed = 0xbc9f1d34
const m = 0xc6a4a793

type Filter []byte

// 对于给定的误判率P和给定的entries数量
// 计算出size的最优解，返回计算hash函数个数k的前提值
// size = bitperkey * entriesNum
// k = bitsperkey * 0.69
func BitsPerkey(entriesNum int, probability float64) int {
	size := -1 * float64(entriesNum) * math.Log(probability) / math.Pow(float64(0.69314718056), 2)
	// 在对于确定的size和n得到hash函数个数K的预处理数据
	locs := math.Ceil(size / float64(entriesNum))
	return int(locs)
}

// 将keys插入到BloomFilter中
func insertFilter(keys []uint32, bitsperkey int) []byte {
	if bitsperkey < 0 {
		bitsperkey = 0
	}
	k := uint32(float64(bitsperkey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	size := uint32(len(keys) * bitsperkey)
	if size < 64 {
		size = 64
	}
	Bytes := (size + 7) / 8
	Bits := Bytes * 8
	filter := make([]byte, Bytes+1)
	for _, hash := range keys { // 将keys中每一个key(hash值)都插入到bloomFilter中
		delta := hash>>17 | hash<<15
		for j := uint32(0); j < k; j++ {
			offset := hash % uint32(Bits)
			// byteOffset表示在filter的那个index
			byteOffset := offset / 8
			// bitOffset表示在filter的index中的第几位
			bitOffset := offset % 8
			filter[byteOffset] |= 1 << bitOffset
			hash += delta
		}
	}
	filter[Bytes] = uint8(k)
	return filter
}

// 创建一个bloomFilter
func NewFilter(keys []uint32, bitperkey int) Filter {
	return Filter(insertFilter(keys, bitperkey))
}

// 计算hash值
func Hash(key []byte) uint32 {
	hash := uint32(seed) ^ uint32(len(key))*m
	// 每次处理key的前四个byte
	for ; len(key) >= 4; key = key[4:] {
		hash += uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		hash *= m
		hash ^= hash >> 16
	}
	// 处理剩下的key
	switch len(key) {
	case 3:
		hash += uint32(key[2]) << 16
		fallthrough
	case 2:
		hash += uint32(key[1]) << 8
		fallthrough
	case 1:
		hash += uint32(key[0])
		hash *= m
		hash ^= hash >> 24
	}
	return hash
}

// 判断是否有可能存在于Bloom Filter
func (f Filter) MayContain(hash uint32) bool {
	if len(f) < 2 {
		return false
	}
	// hash函数的个数
	k := f[len(f)-1]
	bits := uint32(8 * (len(f) - 1))
	delta := hash>>17 | hash<<15
	for j := uint8(0); j < k; j++ {
		// 将hash值取模，保证能放入filter中
		offset := hash % bits
		// byteOffset表示在filter的那个index
		byteOffset := offset / 8
		// bitOffset表示在filter的index中的第几位
		bitOffset := offset % 8
		if f[byteOffset]&(1<<bitOffset) == 0 {
			return false
		}
		// 新的hash值
		hash += delta
	}
	return true
}

// 判断是否可能存在于Bloom Filter
func (f Filter) MayContainKey(key []byte) bool {
	return f.MayContain(Hash(key))
}
