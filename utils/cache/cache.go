package cache

import (
	"container/list"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

/*
	大体的策略是所有的数据都会进入到 Window-LRU 中，当WLRU满了之后会弹出链表末尾的节点W；
	尝试将W放入到 segment-LRU 中，在segmentLRU中会首先进入到Probation中，当Probation中的数据再被访问会加入到Protected；
	W加入到Probation中会通过 CMSketch中的计数来和 Probation的A1作比较；
	当W试图加入到Probation时会首先经过BloomFilter来快速判断是否出现过至少一次；

	一个高频key进入到SLRU之后会很快就加入到A2中，当需要从A2中淘汰，需要很多次替换，并且从SLRU A1中被淘汰还需要经过和WLRU的对比；
	并且从SLRU中被淘汰不会删除计数信息和bloomFilter，所以还是可以很快回到缓存中；

	1. 针对只访问一次的数据，在WLRU中很快就被淘汰了，不会占用缓存空间；
	2. 针对突发性的稀疏流量。就是在短时间内频繁访问的数据，WLRU可以很好的适应这种访问模型；
	3. 针对真正的热点数据，很快就会从WLRU中加入到Protected区，并且会经过保鲜机制一个合理的数量
*/

// WLRU中占据所有空间的多少
const wlruPct = 1

// 基于Window-TinyLFU实现的Cache的数据结构
type Cache struct {
	// 读写锁
	m sync.RWMutex
	// wlru
	wlru *windowLRU
	// slru
	slru *segmentedLRU
	// 快速判断是否至少被访问过一次，用于A1到A2中的过程中的准入策略
	door BloomFilter
	// 计数器
	cs *cmSketch
	// 对Cache的访问数据量
	total int32
	// 需要reset的阈值
	threshold int32
	// 数据存储的map
	data map[uint64]*list.Element
}

type Options struct {
	wlruPct uint8
}

// 根据size创建cache，size指的是需要缓存的个数
// 默认其中1%的空间是wlru，剩下的空间的80%是Protected A2， 20%是Probation A1；
func NewCache(size int) *Cache {
	if size < 3 {
		size = 3
	}
	// 默认占1%
	wlruSize := (wlruPct * size) / 100
	if wlruSize < 1 {
		wlruSize = 1
	}

	// 计算SLRU部分空间
	slruSize := size - wlruSize
	// 其中A1 probation部分占用20%
	a1Size := int(0.2 * float64(slruSize))
	if a1Size < 1 {
		a1Size = 1
	}

	// 创建需要的data
	data := make(map[uint64]*list.Element, size)

	return &Cache{
		// data是共用的，创建对应大小的wlru
		wlru: newWindowLRU(wlruSize, data),
		// data是共用的，创建对应大小的slru
		slru: newSLRU(data, a1Size, size-a1Size-wlruSize),
		// 创建对应误判率和size1的bloomfilter
		door: *newFilter(size, 0.001),
		// 访问数
		total: 0,
		// 计数器
		cs: newCmSketch(int64(size)),
		// 数据
		data: data,
	}
}

// set
func (c *Cache) set(key, value interface{}) bool {
	// keyHash用于快速定位，confliyHash用于判断冲突
	keyHash, conflitHash := c.keyToHash(key)

	// 将key和value封装到item中
	item := storeItem{
		stage:    0,
		key:      keyHash,
		conflict: conflitHash,
		value:    value,
	}

	// 所有的数据都一定要加入到wlru中
	// 如果wlru满了，就要从wlru中淘汰一个item
	eitem, evicted := c.wlru.add(item)

	// 如果不需要淘汰，直接返回
	if !evicted {
		return true
	}
	// 如果需要将wlru中淘汰，就要将这eitem放入到SLRU中，首先会查看SLRU中是否可以直接放入
	// 如果SLRU没有满，返回nil，如果满了返回A1的最后一个item
	vitem := c.slru.victim()

	// 如果A1没有满，将eitem放入到A1中
	if vitem == nil {
		c.slru.add(eitem)
		return true
	}

	// 检查一下wlru中淘汰出来的eitem是否再bllomfilter中，如果存在就说明可能之前被访问过
	// 如果不存在就说明之前肯定没有被访问过，就没有必要放到到slru中
	if !c.door.Allow(uint32(eitem.key)) {
		return true
	}

	// 如果之前放到过slru中，说明之前可能被访问过多次，就要进一步做准入策略判断是否能替换进去
	// 从cmSketch中获取到 两个item的大概的访问数
	vcount := c.cs.GetEstimate(vitem.key)
	ocount := c.cs.GetEstimate(eitem.key)
	// 如果slru A1中需要被替换出来的vitem的被访问次数 大于 wlru中需要放入slru中的被访问次数，就不替换
	if vcount > ocount {
		return true
	}
	// 如果通过了准入策略，就允许替换，之前在slru中的vitem会被删掉
	c.slru.add(eitem)
	return true
}

// 暴露的接口，通过加写锁保证写缓存的有效性质
func (c *Cache) Set(key, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

// get
// 根据key查找对应的value
func (c *Cache) get(key interface{}) (interface{}, bool) {
	// 首先将cache计数+1
	c.total++
	// 判断是否需要reset
	if c.total >= c.threshold {
		c.cs.Reset()
		c.door.reset()
		c.total = 0
	}

	// keyHash用于快速定位，confliyHash用于判断冲突
	keyHash, conflitHash := c.keyToHash(key)
	// 尝试获取对应的element
	element, ok := c.data[keyHash]
	// 如果缓存中不存在，在缓存的bloomfilter中插入记录(表示至少被访问过一次)，在计数器中自增
	// 返回nil，和没有找到false
	if !ok {
		c.door.Allow(uint32(keyHash))
		c.cs.Increment(keyHash)
		return nil, false
	}

	// 如果找到了key对应的element，获取到对应的item
	item := element.Value.(*storeItem)
	// 校验，如果不存在，说明找到的是错误的数据
	if item.conflict != conflitHash {
		c.door.Allow(uint32(keyHash))
		c.cs.Increment(keyHash)
		return nil, false
	}

	// 如果校验找到了正确的item，标记一下，再自增计数；
	c.door.Allow(uint32(keyHash))
	c.cs.Increment(item.key)
	val := item.value
	if item.stage == 0 {
		// 如果在wlru中，会调整wlru中的数据
		c.wlru.get(element)
	} else {
		// 如果在slru中，调整在slru中的数据
		c.slru.get(element)
	}
	// 返回value
	return val, true
}

// 暴露的接口，通过加读锁保证读缓存的一致性
func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

// del
// 在Cache中删除一个key
func (c *Cache) del(key interface{}) (interface{}, bool) {
	// keyHash用于快速定位，confliyHash用于判断冲突
	keyHash, conflitHash := c.keyToHash(key)

	element, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}
	// 检查是否有效
	item := element.Value.(*storeItem)
	if conflitHash != 0 && (conflitHash != item.conflict) {
		return 0, false
	}
	//只会在data中删除，但是bloomfilter和cmSketch的记录不会被删除，记录信息只会在reset的时候被清理
	delete(c.data, keyHash)
	return item.conflict, true
}

// 暴露的接口，通过加写锁保证写缓存的一致性
func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

// hash()
type stringStruct struct {
	str unsafe.Pointer
	len int
}

// 通过//go:noescape运行在栈上，提升运行效率
// 链接到runtime.memhash()
//
//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString 是Go用于map的hash函数，每一个进程都会不一样，不可用用于持久化的散列中，但是这里只是用于缓存的业务环境，所以没有影响
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// 类型判断做hash
func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

// test
func (c *Cache) String() string {
	var s string
	s += c.wlru.String() + " | " + c.slru.String()
	return s
}
