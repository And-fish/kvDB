package utils

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"unsafe"
)

const maxLevel = 20
const oneLevelSize = int(unsafe.Sizeof(uint32(0)))
const levelIncrease = math.MaxUint32 / 2

type skiplistNode struct {
	// 高32位是size，低32位是offset
	value     uint64
	keyoffset uint32
	keysize   uint16
	height    uint16
	level     [maxLevel]uint32
}
type SkipList struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	onClose    func()
}

func (s *SkipList) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}
func (s *SkipList) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	// 如果没有被引用，开始释放程序
	if s.onClose != nil {
		s.onClose()
	}
	s.arena = nil
}
func encodingValAsInfo(valOffset, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}
func decodeValFromInfo(info uint64) (valoffset, valsize uint32) {
	valoffset = uint32(info)
	valsize = uint32(info >> 32)
	return
}
func newNode(arena *Arena, key []byte, val ValueStruct, height int) *skiplistNode {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	valOffset := arena.putVal(val)
	valInfo := encodingValAsInfo(valOffset, val.ValEncodedSize())

	node := arena.getNode(nodeOffset)
	node.value = valInfo
	node.keyoffset = keyOffset
	node.keysize = uint16(len(key))
	node.height = uint16(height)
	return node
}
func NewSkiplist(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	// 头节点直接分配 maxLevel层的空间
	head := newNode(arena, nil, ValueStruct{}, maxLevel)
	headOffset := arena.getNodeOffset(head)
	return &SkipList{
		height:     1, // 但是实际上暂时只使用头节点的1层
		headOffset: headOffset,
		ref:        1,
		arena:      arena,
	}
}

// 在指定的Arena中，获取[]byte类型的skiplistNode key
func (n *skiplistNode) getKey(arena *Arena) []byte {
	return arena.getKey(n.keyoffset, n.keysize)
}

// 获取skiplistNode的valoffset和valsize
func (n *skiplistNode) getValueMetaData() (valoffset, valsize uint32) {
	info := atomic.LoadUint64(&n.value)
	valoffset, valsize = decodeValFromInfo(info)
	return
}
func (n *skiplistNode) setValue(arena *Arena, valueInfo uint64) {
	atomic.StoreUint64(&n.value, valueInfo)
}
func (n *skiplistNode) getNextNodeOffset(height int) uint32 {
	return atomic.LoadUint32(&n.level[height])
}
func (n *skiplistNode) casNextNodeOffset(height int, old, new uint32) bool {
	return atomic.CompareAndSwapUint32(&n.level[height], old, new)
}
func (n *skiplistNode) getValueStruct(arena *Arena) ValueStruct {
	valoffset, valsize := n.getValueMetaData()
	return arena.getVal(valoffset, valsize)
}

//
//go:linkname FastRand runtime.fastrand
func FastRand() uint32
func (s *SkipList) randomLevel() int {
	h := 1
	for h < maxLevel && FastRand() <= levelIncrease {
		h++
	}
	return h
}

func (s *SkipList) getNextNode(node *skiplistNode, height int) *skiplistNode {
	return s.arena.getNode(node.getNextNodeOffset(height))
}
func (s *SkipList) getHead() *skiplistNode {
	return s.arena.getNode(s.headOffset)
}
func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// 0 if key1 == key2,
// -1 if key1 < key2,
// +1 if key1 > key2.
func CompareKeys(key1, key2 []byte) (res int) {
	CondPanic(len(key1) <= 8 || len(key2) <= 8, fmt.Errorf("%s,%s <8", string(key1), string(key2)))
	res = bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8])
	if res != 0 {
		return
	}
	res = bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
	return
}

// 按照less和allowEqual两个参数获取到一个最接近key的node
// (false,false)：	find a 最接近key的 node，node.key > key；
// (false,true)：	find a 最接近key的 node，node.key >= key；
// (true,false)：	find a 最接近key的 node，node.key < key；
// (true,true)：	find a 最接近key的 node，node.key <= key；
func (s *SkipList) findNear(key []byte, less, allowEqual bool) (*skiplistNode, bool) {
	cur := s.getHead()
	height := int(s.height - 1)
	for {
		nextNode := s.getNextNode(cur, height)
		// 如果遇到某个node在某一层没有nextNode
		if nextNode == nil {
			// 如果还不是最后一层，就再看看下一层
			if height > 0 {
				height--
				continue
			}
			// 如果是最后一层，说明skiplist中没有 >= key的node
			if !less {
				// 如果要找Node.key > key || Node.key >= key的节点，返回nil
				return nil, false
			}
			// 检查skiplist中是否有节点，因为FirstNode是手动创建的
			if cur == s.getHead() {
				return nil, false
			}
			// 如果要找Node.key < key || Node.key <= key的节点，且当前已经是第0层，且当前节点cur不是头节点，返回cur
			// 因为在cur.before节点已经判断过 cur.key ?= key，所以还要返回false
			return cur, false
		}

		nextKey := nextNode.getKey(s.arena)
		cmp := CompareKeys(key, nextKey)
		// 说明key > nextkey
		if cmp > 0 {
			// 同层往后找
			cur = nextNode
			continue
		}
		// 说明key == nextkey
		if cmp == 0 {
			// 如果要找的node.key允许相等，而相等的key是最接近key的
			if allowEqual {
				return nextNode, true
			}
			// 如果不允许相等，且要找nodekey > key的node
			if !less {
				// 返回nodekey == key的node的第0层的下一个node
				return s.getNextNode(nextNode, 0), false
			}
			// 如果要找nodekey < key的node，且当前不是最后一层
			if height > 0 {
				// 往下层找
				// 1-A- -4
				// 1-2-3-4
				// 1-2-3-4
				// 假设当前cur为A，nextnode.key == key ==4；跳到下一层检查是否有更近的
				height--
				continue
			}
			// 如果skiplist为空
			if cur == s.getHead() {
				return nil, false
			}
			// 如果skiplist不为空
			return cur, false
		}
		// cmp < 0，说明key < nextkey
		// 如果当前不是最后一层，继续往下面看
		if height > 0 {
			height--
			continue
		}
		// 如果是最后一层，且要找nodekey > key || nodekey >= key的node
		if !less {
			return nextNode, false
		}
		//如果skiplist为空
		if cur == s.getHead() {
			return nil, false
		}
		// 如果是最后一层，且要找nodekey < key || nodekey <= key的node
		return cur, false
	}
}

// 从beforNpde开始在level层找到一个适合key insert的index，beforeKey < Key < nextNode
// 返回beforeOffset 和 nextNodeOffset
func (s *SkipList) findInsertForLevel(key []byte, beforeNodeOffset uint32, level int) (uint32, uint32) {
	for {
		beforeNode := s.arena.getNode(beforeNodeOffset)
		nextNodeOffset := beforeNode.getNextNodeOffset(level)
		nextNode := s.arena.getNode(nextNodeOffset)
		if nextNode == nil {
			return beforeNodeOffset, nextNodeOffset
		}
		nextKey := nextNode.getKey(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			// 如果找到了相等的节点

			return nextNodeOffset, nextNodeOffset
		}
		// 如果找到了某个nextNodeKey > Key，说明 beforeKey < key < nextKey
		if cmp < 0 {
			return beforeNodeOffset, nextNodeOffset
		}
		// 如果nextKey < key，继续找
		beforeNodeOffset = nextNodeOffset
	}
}
func (s *SkipList) Add(e *Entry) {
	key := e.Key
	val := ValueStruct{
		Meta:    e.Meta,
		Value:   e.Value,
		TTL:     e.TTL,
		Version: e.Version,
	}

	sheight := s.getHeight()
	var prevNodes [maxLevel + 1]uint32
	var nextNodes [maxLevel + 1]uint32
	prevNodes[sheight] = s.headOffset
	for i := int(sheight) - 1; i >= 0; i-- {
		prevNodes[i], nextNodes[i] = s.findInsertForLevel(key, prevNodes[i+1], i)
		if prevNodes[i] == nextNodes[i] {
			valueOffset := s.arena.putVal(val)
			valueCode := encodingValAsInfo(valueOffset, val.ValEncodedSize())
			oldNode := s.arena.getNode(prevNodes[i])
			oldNode.setValue(s.arena, valueCode)
			return
		}
	}

	nodeLevel := s.randomLevel()
	newNode := newNode(s.arena, key, val, nodeLevel)
	// 支持并发修改，所以再获取一次最新的sheight
	sheight = s.getHeight()
	// 如果sheight只增不减，如果有更大的就不需要更新
	for nodeLevel > int(sheight) {
		if atomic.CompareAndSwapInt32(&s.height, sheight, int32(nodeLevel)) {
			break
		}
		// 如果cas失败，说明当前height被修改了，重新判断是否需要更新
		sheight = s.getHeight()
	}

	// 插入
	// 从第0层开始插入，这样如果并发插入同一个key时可以较早发现，减少调整操作
	for i := 0; i < nodeLevel; i++ {
		for {
			// 因为nodeLevel可能比之前的node大
			if s.arena.getNode(prevNodes[i]) == nil {
				// 初始化一下prevNode 和 nextNode
				prevNodes[i], nextNodes[i] = s.findInsertForLevel(key, s.headOffset, i)
			}
			// 将第i层的下一个node设置为nextNodes[i]
			newNode.level[i] = nextNodes[i]
			prevNode := s.arena.getNode(prevNodes[i])
			// 原子的将prevNode的next[i]换为newNode
			if prevNode.casNextNodeOffset(i, nextNodes[i], s.arena.getNodeOffset(newNode)) {
				break
			}
			// 如果没换成，说明有另一个并发操作已经在同一个位置进行insert
			// 重新获取本层插入信息
			prevNodes[i], nextNodes[i] = s.findInsertForLevel(key, prevNodes[i], i)
			// 如果先插入的node的key和newNode一样，会检查为 prevNodes[i] == nextNodes[i]
			if prevNodes[i] == nextNodes[i] {
				// 先判断一下当前是否在第0层
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				// 直接将先来的insert的value修改为当前的val
				valueOffset := s.arena.putVal(val)
				valueCode := encodingValAsInfo(valueOffset, val.ValEncodedSize())
				oldNode := s.arena.getNode(prevNodes[i])
				oldNode.setValue(s.arena, valueCode)
				return
			}
			// 如果不是同一个key，就重试插入
		}
	}
}
func (s *SkipList) Search(key []byte) ValueStruct {
	res := ValueStruct{}
	// 尝试去找到一个大于等于key的node
	node, _ := s.findNear(key, false, true)
	// 没有找到
	if node == nil {
		return res
	}
	key2 := s.arena.getKey(node.keyoffset, node.keysize)
	if !IsSameKey(key, key2) {
		return res
	}

	valOffset, valSize := node.getValueMetaData()
	res = s.arena.getVal(valOffset, valSize)
	res.TTL = ParseTimeStamp(key2)
	return res
}
func (s *SkipList) findLast() *skiplistNode {
	// 从头节点开始
	node := s.getHead()
	// 从最高层开始
	level := int(s.getHeight() - 1)
	for {
		nextNode := s.getNextNode(node, level)
		// 如果在某一层没有走到最后，一直往右走
		if nextNode != nil {
			node = nextNode
			continue
		}
		// 如果到了第0层 且 在第0层判断skiplist是不是空的
		if level == 0 {
			if node == s.getHead() {
				return nil
			}
			// 如果不为空，返回node
			return node
		}
		// 如果在某一非0层走到头，继续从下一层开始
		level--
	}
}

func (sl *SkipList) GetSize() int64 {
	return sl.arena.szie()
}

// Draw plot Skiplist, align represents align the same node in different level
func (s *SkipList) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNextNode(next, level)
			if next != nil {
				key := next.getKey(s.arena)
				vs := next.getValueStruct(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}
