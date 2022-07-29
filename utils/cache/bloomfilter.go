package cache

// 重写bloomfilter的原因是为了将cache功能解耦出来，单独做库
import (
	"math"
)

// hash函数的随机种子
const Seed = 0xbc9f1d34
const M = 0xc6a4a793

type Filter []byte

// 封装
type BloomFilter struct {
	bitmap Filter
	k      uint8
}

// 对于给定的误判率P和给定的entries数量
// 计算出size的最优解，返回计算hash函数个数k的前提值
// size = bitperkey * entriesNum
// k = bitsperkey * 0.69
func bitsPerkey(entriesNum int, probability float64) int {
	size := -1 * float64(entriesNum) * math.Log(probability) / math.Pow(float64(0.69314718056), 2)
	// 在对于确定的size和n得到hash函数个数K的预处理数据
	locs := math.Ceil(size / float64(entriesNum))
	return int(locs)
}

// 根据entriesNum 和 bitperkey 初始化Filter
func initFilter(entriesNum, bitsperkey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsperkey < 0 {
		bitsperkey = 0
	}
	// 计算hash函数数量k，至少为1，最高为30
	k := uint32(float64(bitsperkey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bf.k = uint8(k)

	// BllomFilter的最小size是64
	size := entriesNum * bitsperkey
	if size < 64 {
		size = 64
	}
	// 将需要的bitSize转化为byteSize
	Bytes := (size + 7) / 8
	// 创建对应大小的[]byte，最后一位是k
	filter := make([]byte, Bytes+1)
	filter[Bytes] = uint8(k)
	bf.bitmap = filter

	return bf
}

// 根据entriesNum 和 误判率probabitity 创建新的BloomFilter
func newFilter(entriesNum int, probability float64) *BloomFilter {
	// 计算bitsperkey
	bitperkey := bitsPerkey(entriesNum, probability)
	// 根据entriesNum 和 bitsperkey创建新的BloomFilter
	return initFilter(entriesNum, bitperkey)
}

// 插入某个key
func (bf *BloomFilter) InsertKey(key []byte) {
	// 计算hash值并执行插入操作
	bf.Insert(Hash(key))
	return
}

// 根据给出的hash值，执行插入操作
func (bf *BloomFilter) Insert(hash uint32) {
	// 获得key
	k := bf.k
	if k > 30 {
		return
	}

	// 实际Filter的size
	size := uint32(8 * (bf.Len() - 1))
	delta := hash>>17 | hash<<15
	for j := uint8(0); j < k; j++ {
		offset := hash % uint32(size)
		byteOffset := offset / 8
		bitOffset := offset % 8
		// 将对应位置置为1
		bf.bitmap[byteOffset] |= 1 << bitOffset
		// 计算为新的hash值
		hash += delta
	}
	return
}

// 全部置为0
func (bf *BloomFilter) reset() {
	if bf == nil {
		return
	}
	for i := range bf.bitmap {
		bf.bitmap[i] = 0
	}
}

// 一个类似于Murmue hash的Hash函数
func Hash(key []byte) uint32 {
	hash := uint32(Seed) ^ uint32(len(key))*M
	// 每次处理key的前四个byte
	for ; len(key) >= 4; key = key[4:] {
		hash += uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		hash *= M
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
		hash *= M
		hash ^= hash >> 24
	}
	return hash
}

// 通过hash值检查bloomfilter中是否记录过这个数据
func (bf *BloomFilter) MayContain(hash uint32) bool {
	if bf.Len() < 2 {
		return false
	}
	// hash函数的个数
	k := bf.k
	bits := uint32(8 * (bf.Len() - 1))
	delta := hash>>17 | hash<<15
	for j := uint8(0); j < k; j++ {
		// 将hash值取模，保证能放入filter中
		offset := hash % bits
		// byteOffset表示在filter的那个index
		byteOffset := offset / 8
		// bitOffset表示在filter的index中的第几位
		bitOffset := offset % 8
		// 如果在检查的过程中遇到了某个为0，返回false
		if bf.bitmap[byteOffset]&(1<<bitOffset) == 0 {
			return false
		}
		// 新的hash值
		hash += delta
	}
	return true
}

// 通过key检查bloomfilter中是否记录过这个数据
func (bf *BloomFilter) MayContainKey(key []byte) bool {
	// 转化为hash值再检查
	return bf.MayContain(Hash(key))
}

// filter的长度
func (bf *BloomFilter) Len() int32 {
	return int32(len(bf.bitmap))
}

// 通过hash检查是否存在，如果不存在就插入，返回是否存在
func (bf *BloomFilter) Allow(hash uint32) bool {
	if bf == nil {
		return true
	}
	already := bf.MayContain(hash)
	if !already {
		bf.Insert(hash)
	}
	return already
}

// 通过key检查是否存在，如果不存在就插入，返回是否存在
func (bf *BloomFilter) AllowKey(key []byte) bool {
	if bf == nil {
		return true
	}
	already := bf.MayContainKey(key)
	if !already {
		bf.InsertKey(key)
	}
	return already
}
