package utils

import (
	"sync/atomic"
	"unsafe"
)

const maxNodeSize = int(unsafe.Sizeof(skiplistNode{}))
const alignOffset = int(unsafe.Sizeof(uint8(0)))

type Arena struct {
	n       uint32
	growing bool
	buf     []byte
}

func newArena(size int64) *Arena {
	res := &Arena{
		n:   1,
		buf: make([]byte, size),
	}
	return res
}

func (a *Arena) allocate(size uint32) uint32 {
	newoffset := atomic.AddUint32(&a.n, size)
	if a.growing {
		return newoffset - size
	}
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

func (a *Arena) putNode(height int) uint32 {
	unusedSize := (maxLevel - height) * oneLevelSize
	allocateSize := maxNodeSize - unusedSize + alignOffset
	offset := a.allocate(uint32(allocateSize))
	rwOffset := (offset + uint32(alignOffset)) &^ uint32(alignOffset)
	return rwOffset
}
func (a *Arena) getNode(offset uint32) *skiplistNode {
	return (*skiplistNode)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) putKey(key []byte) uint32 {
	keysize := len(key)
	offset := a.allocate(uint32(keysize))
	keybuf := a.buf[alignOffset : alignOffset+keysize]
	copy(keybuf, key)
	return offset
}
func (a *Arena) getKey(keyOffset uint32, keySize uint16) []byte {
	return a.buf[keyOffset : keyOffset+uint32(keySize)]
}
func (a *Arena) putVal(val ValueStruct) uint32 {
	size := val.ValEncodedSize()
	offset := a.allocate(size)
	val.ValEncoding(a.buf[offset:])
	return offset
}
func (a *Arena) getVal(ValOffset, Valsize uint32) ValueStruct {
	res := ValueStruct{}
	res.ValDecode(a.buf[ValOffset : ValOffset+Valsize])
	return res
}
func (a *Arena) getNodeOffset(node *skiplistNode) uint32 {
	if node == nil {
		return 0
	}
	// 因为buf是连续的数组，所以将node的地址和buf的头节点的地址相见就是node在buf上的offset
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.buf[0])))
}
