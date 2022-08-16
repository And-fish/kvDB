package utils

import (
	"sync/atomic"
	"unsafe"
)

const maxNodeSize = int(unsafe.Sizeof(skiplistNode{}))
const alignOffset = int(unsafe.Sizeof(uint64(0))) - 1

// Arena结构
type Arena struct {
	n       uint32
	growing bool
	buf     []byte
}

// 根据size创建一个新的Arena
func newArena(size int64) *Arena {
	res := &Arena{
		n:   1,
		buf: make([]byte, size),
	}
	return res
}

// 在Arena中分配一块足够的大小，返回这块空间的起始写入offset
func (a *Arena) allocate(size uint32) uint32 {
	newoffset := atomic.AddUint32(&a.n, size)
	if !a.growing {
		AssertTrue(int(newoffset) <= len(a.buf))
		return newoffset - size
	}
	// 扩容
	if int(newoffset) > len(a.buf)-maxNodeSize {
		newbufsize := uint32(len(a.buf))
		if newbufsize > (1 << 30) {
			newbufsize = (1 << 30)
		}
		if newbufsize < size {
			newbufsize = size
		}
		newbuf := make([]byte, int(newbufsize)+len(a.buf))
		a.buf = newbuf
	}
	return newoffset - size
}

// 放入node(会根据实际的height去分配正好足够的空间，减少空间浪费)
func (a *Arena) putNode(height int) uint32 {
	unusedSize := (maxLevel - height) * oneLevelSize
	allocateSize := maxNodeSize - unusedSize + alignOffset // 加上对齐偏移量
	// 请求实际的空间大小 (增加对齐冗余后的)
	offset := a.allocate(uint32(allocateSize))
	// 计算实际开始写入的偏移量
	rwOffset := (offset + uint32(alignOffset)) &^ uint32(alignOffset) // 会使得rwOffset的是8的倍数，增加读写效率
	return rwOffset
}

// 根据offset获取node
func (a *Arena) getNode(offset uint32) *skiplistNode {
	if offset == 0 {
		return nil
	}

	return (*skiplistNode)(unsafe.Pointer(&a.buf[offset])) // 重构为SkipListNode
}

// 放入key
func (a *Arena) putKey(key []byte) uint32 {
	keysize := uint32(len(key))
	offset := a.allocate(uint32(keysize)) // 申请到足够大小的空间
	keybuf := a.buf[offset : offset+keysize]
	AssertTrue(len(key) == copy(keybuf, key)) // 全量复制
	return offset
}

// 获取key
func (a *Arena) getKey(keyOffset uint32, keySize uint16) []byte {
	return a.buf[keyOffset : keyOffset+uint32(keySize)] // truncate
}

// 放入value，返回写入起始的地址	(传入的是ValueStruct，也就是mete + value + TTL)
func (a *Arena) putVal(val ValueStruct) uint32 {
	size := val.ValEncodedSize()
	offset := a.allocate(size)
	val.ValEncoding(a.buf[offset:])
	return offset
}

// 获取value
func (a *Arena) getVal(ValOffset, Valsize uint32) ValueStruct {
	res := ValueStruct{}
	res.ValDecode(a.buf[ValOffset : ValOffset+Valsize])
	return res
}

// 获取某个node在Arena上的offset
func (a *Arena) getNodeOffset(node *skiplistNode) uint32 {
	if node == nil {
		return 0
	}
	// 因为buf是连续的数组，所以将node的地址和buf的头节点的地址相减就是node在buf上的offset
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

// 返回arena的被使用了的大小 (NOT LEN(ARENA.BUF))
func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.n))
}
