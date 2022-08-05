package utils

// 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}
type Item interface {
	Entry() *Entry
}
type Options struct {
	Prefix []byte
	IsAsc  bool // 是否是升序查询
}

// SkipListIterator
type SkipListIterator struct {
	skiplist *SkipList
	node     *skiplistNode
}

// 返回迭代器当前的node的key
func (si *SkipListIterator) Key() []byte {
	return si.skiplist.arena.getKey(si.node.keyoffset, si.node.keysize)
}

// 返回迭代器当前node的value
func (si *SkipListIterator) Value() ValueStruct {
	valoffset, valsize := si.node.getValueMetaData()
	return si.skiplist.arena.getVal(valoffset, valsize)
}

// 返回迭代器当前node的value，以Uint64格式返回
func (si *SkipListIterator) ValueUint64() uint64 {
	return si.node.value
}

// 跳转到第一个Node
func (si *SkipListIterator) SeekToFirst() {
	si.node = si.skiplist.getNextNode(si.skiplist.getHead(), 0)
}

// 跳转到最后一个Node
func (si *SkipListIterator) SeekToLast() {
	si.node = si.skiplist.findLast()
}

// 创建一个新的迭代器
func (s *SkipList) NewSkiplistIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{skiplist: s}
}

// 关闭迭代器
func (si *SkipListIterator) Close() error {
	si.skiplist.DecrRef()
	return nil
}
func (si *SkipListIterator) Next() {
	AssertTrue(si.Valid())
	si.node = si.skiplist.getNextNode(si.node, 0)

}
func (si *SkipListIterator) Prev() {
	if si.Valid() {
		// 找一个最接近的小于key的node
		si.node, _ = si.skiplist.findNear(si.Key(), true, false)
	}
}

// 判断是否还有效
func (si *SkipListIterator) Valid() bool {
	// 如果迭代器遍历完毕就认为无效
	return si.node != nil
}

// 从头开始
func (si *SkipListIterator) Rewind() {
	si.SeekToFirst()
}

// 返回当前的项
func (si *SkipListIterator) Item() Item {
	return &Entry{
		Key:     si.Key(),
		Value:   si.Value().Value,
		TTL:     si.Value().TTL,
		Meta:    si.Value().Meta,
		Version: si.Value().Version,
	}
}

// 找一个最接近targetKey且node.key >= key的node
func (si *SkipListIterator) SeekMore(targetKey []byte) {
	si.node, _ = si.skiplist.findNear(targetKey, false, true)
}

// 找一个最接近targetKey且node.key <= key的node
func (si *SkipListIterator) SeekLess(targetKey []byte) {
	si.node, _ = si.skiplist.findNear(targetKey, true, true)
}

// 找到一个最接近key，且node.key >= key 的node
func (si *SkipListIterator) Seek(key []byte) {
	si.node, _ = si.skiplist.findNear(key, false, true)
}
