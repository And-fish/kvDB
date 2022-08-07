package cache

import (
	"fmt"
	"math/rand"
	"time"
)

// 需要4次计数冗余，减少hash冲突带来的影响
const cmDepth = 4

// 一行计数槽
type cmRow []byte

// 真正用于做对比的数据结构
type cmSketch struct {
	// 需要 cmDepth的计数冗余
	rows [cmDepth]cmRow
	// 每一层对应的hash 种子
	seed [cmDepth]uint64
	// 用于计算的掩码，mask+1 == counter的数量
	mask uint64
}

// 根据所需要的numCounters 计数器数量来新建一个cmRow
func newCmRow(numCounters int64) cmRow {
	// 用4个bit来表示count，一个byte可以表示两个counter，只需要创建numCounter/2 bytes的[]btye
	return cmRow(make([]byte, numCounters/2))
}

// 对n做自增计数
func (r cmRow) incrRow(n uint64) {
	// 找到对应的byteOffset
	byteIndex := n / 2
	// 如果n为奇数需要右移4位，偶数不需要右移，等价于 (n%2)*4
	bitIndex := (n & 1) * 4

	// 获取到对应的计数，例如 r[byteIndex] == 1101 1001，
	// 如果要找高4位，右移4位 1101，& 0b1111，count == 0b1101
	// 如果要找低4位，右移0位 1101 1001，& 0b1111，count == 0b1001
	count := (r[byteIndex] >> (bitIndex)) & 0x0f

	// count被设置位最高15，所以只会对count<15的时候自增计数
	if count < 15 {
		// 例如 r[byteIndex] == 1101 1001，需要对高4位自增，1101 1001 + 1 0000 =1110 0000，0b1110 = 0b1101 + 1
		r[byteIndex] += 1 << bitIndex
	}
}

// 查询对应的计数
func (r cmRow) getRow(n uint64) uint8 {
	byteIndex := n / 2
	bitIndex := (n & 1) * 4
	return (r[byteIndex] >> (bitIndex)) & 0x0f
}

// 保鲜机制，方便window中的数据可以更容易加入到SLRU
func (r cmRow) reset() {
	for i := range r {
		// 例如 r[i] == 1101 1001，
		// 1101 1001 >> 1 == 0110 1100 先除二；
		// 0110 1100 & 0111 0111 == 0110 0100，可以保证第4位和第8位的一定是0
		r[i] = r[i] >> 1 & 0x77
	}
}

// 清空
func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

// test
func (r cmRow) string() string {
	var res string
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		res += fmt.Sprintf("%02d", (r[i/2]>>((i&1)*4))&0x77)
	}
	res = res[:len(res)-1]
	return res
}

// 找到一个最接近的二次幂
func next2Power(x uint64) uint64 {
	// 首先减一，可以保证一个二次幂的数能最后找到自己
	x--
	// 因为最高有效位肯定是1，x |= x>>1可以保证前两位都是1，后面操作同理
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	// 此时可以保证所有有效位都是1，x就只有最高位是1
	x++
	return x
}

// 一个新的cmSketch
func newCmSketch(numCounters int64) *cmSketch {
	// numCounters只能大于零
	if numCounters <= 0 {
		panic("cmSketch: invalid numCounters")
	}

	// 找到一个二次幂的数作为numCounters
	numCounters = int64(next2Power(uint64(numCounters)))
	cmsketch := &cmSketch{
		mask: uint64(numCounters) - 1,
	}

	// 设置种子生成器
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 设置对应计数冗余的 种子和cmRow
	for i := 0; i < cmDepth; i++ {
		cmsketch.rows[i] = newCmRow(numCounters)
		cmsketch.seed[i] = source.Uint64()
	}
	return cmsketch
}

// 每一层都做自增操作
func (cs *cmSketch) Increment(hash uint64) {
	for i := range cs.rows {
		// 计算hash值并取模
		cs.rows[i].incrRow((hash ^ cs.seed[i]) & cs.mask)
	}
}

// 获取到计数，最接近正确值的计数可以认为是最小的那个
func (cs *cmSketch) GetEstimate(hash uint64) uint64 {
	min := uint8(255)
	for i := range cs.rows {
		val := cs.rows[i].getRow((hash ^ cs.seed[i]&cs.mask))
		if val < uint8(min) {
			min = val
		}
	}
	return uint64(min)
}

// 每一层减半
func (cs *cmSketch) Reset() {
	for _, r := range cs.rows {
		r.reset()
	}
}

// 每一层清空
func (cs *cmSketch) Clear() {
	for _, r := range cs.rows {
		r.clear()
	}
}
