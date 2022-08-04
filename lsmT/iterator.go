package lsmt

import (
	"bytes"
	"fmt"
	"io"
	"kvdb/pb"
	"kvdb/utils"
	"sort"
)

type Item struct {
	entry *utils.Entry
}

type Iterator struct {
	item     Item
	iterator []utils.Iterator
}

func (item *Item) Entry() *utils.Entry {
	return item.entry
}

// blockIterator用于抽象化跳转去每个block
type blockIterator struct {
	data         []byte
	idx          int
	err          error
	basekey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID    uint64
	blockID    int
	preOverlap uint16

	item utils.Item
}

// 向迭代器中添加block
func (bitr *blockIterator) setBlcok(block *block) {
	bitr.block = block
	bitr.err = nil
	bitr.idx = 0
	bitr.basekey = block.baseKey[:0]
	bitr.preOverlap = 0
	bitr.key = block.baseKey[:0]
	bitr.val = bitr.val[:0]
	// 只需要将entryIndex放进来，剩下的细节不需要在BlockIterator中定义
	bitr.data = block.data[:block.entriesIndexStart]
	bitr.entryOffsets = block.entryOffsets
}

// SetIndx，将迭代器调整到对应的entry上，并记录到迭代器中
func (bitr *blockIterator) setIdx(i int) {
	bitr.idx = i
	if i >= len(bitr.entryOffsets) || i < 0 {
		bitr.err = io.EOF
		return
	}
	bitr.err = nil
	startOffset := bitr.entryOffsets[i]
	if len(bitr.basekey) == 0 {
		var baseHeader header
		baseHeader.decode(bitr.data)
		bitr.basekey = bitr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	if bitr.idx+1 == len(bitr.entryOffsets) {
		endOffset = len(bitr.data)
	} else {
		endOffset = int(bitr.entryOffsets[bitr.idx+1])
	}

	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				bitr.tableID, bitr.blockID, bitr.idx, len(bitr.data), startOffset, endOffset,
				len(bitr.entryOffsets), bitr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	/*
		entry：外 ---> 内
		+-----------------------------------------------+
		| value | ttl | meta | diffkey | diff | overlap |
		+-----------------------------------------------+
	*/
	entryData := bitr.data[startOffset:endOffset]
	var header header
	header.decode(entryData) // 0~4
	if header.overlap > bitr.preOverlap {
		bitr.key = append(bitr.key[:bitr.preOverlap], bitr.basekey[bitr.preOverlap:header.overlap]...)
	}

	bitr.preOverlap += header.overlap
	valueOffset := headerSize + header.diff
	diffkey := entryData[headerSize:valueOffset]
	bitr.key = append(bitr.key[:header.overlap], diffkey...)
	entry := &utils.Entry{Key: bitr.key}
	val := &utils.ValueStruct{}
	val.ValDecode(entryData[valueOffset:])
	entry.Value = val.Value
	entry.TTL = val.TTL
	entry.Meta = val.Meta
	bitr.item = &Item{entry: entry}
}

// 将blockIterator跳转到某个entry，entry.key 正好>= key
func (bitr *blockIterator) seek(key []byte) {
	bitr.err = nil
	startIndex := 0
	// 找到一个entry的key正好 >=  key，返回它在entryOffset的index
	foundEntryIdx := sort.Search(len(bitr.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		bitr.setIdx(idx)
		// 如果bitr.key >= key 返回true
		return utils.CompareKeys(bitr.key, key) >= 0
	})
	bitr.setIdx(foundEntryIdx)
}

// 将blockIterator跳转到第一个entry
func (bitr *blockIterator) seekToFirst() {
	bitr.setIdx(0)
}

// 将blockIterator跳转到最后一个个entry
func (bitr *blockIterator) seekToLast() {
	bitr.setIdx(len(bitr.entryOffsets) - 1)
}

// 跳转到下一个
func (bitr *blockIterator) Next() {
	bitr.setIdx(bitr.idx + 1)
}

// Error
func (bitr *blockIterator) GetError() error {
	return bitr.err
}

// 判断是否还能继续(有效性)
func (bitr *blockIterator) Valid() bool {
	return bitr.err != io.EOF
}

// 从头开始
func (bitr *blockIterator) Rewind() bool {
	bitr.seekToFirst()
	return true
}

// 获取当前的Item，可以根据这个获取到Entry
func (bitr *blockIterator) GetItem() utils.Item {
	return bitr.item
}

// 获取err
func (bitr *blockIterator) Error() error {
	return bitr.err
}
func (bitr *blockIterator) Close() error {
	return nil
}

// tableIterator用于抽象跳转到不同的block
type tableIterator struct {
	item          utils.Item
	opt           *utils.Options
	table         *table
	blockPos      int // 当前是哪一个block
	blockIterator *blockIterator
	err           error
}

// 下一个entry
func (titr *tableIterator) Next() {
	titr.err = nil

	/*
		外 --> 内
		+-------------------------------------------------------------------+
		| ckecksum_len | checksum | BlockIndexs_len | BlockIndexs | BlockData |
		+-------------------------------------------------------------------+
	*/

	// titr.table.sst.GetIndexs().GetOffsets()是table对应的sstable的[]*blockindex
	if titr.blockPos >= len(titr.table.sst.GetIndexs().GetOffsets()) {
		titr.err = io.EOF
		return
	}
	// 如果block.data == nil，说明需要跳到下一个block上
	if len(titr.blockIterator.data) == 0 {
		// 获取block
		block, err := titr.table.block(titr.blockPos)
		if err != nil {
			titr.err = nil
			return
		}
		titr.blockIterator.tableID = titr.table.fid
		titr.blockIterator.blockID = titr.blockPos
		titr.blockIterator.setBlcok(block)
		titr.blockIterator.seekToFirst()
		titr.err = titr.blockIterator.Error()
		return
	}

	titr.blockIterator.Next()
	// 如果无效了，再次为了能够正确迭代，下一次调用Next()会跳转到下一个block
	if !titr.blockIterator.Valid() {
		titr.blockPos++
		titr.blockIterator.data = nil
		titr.Next()
		return
	}
	titr.item = titr.blockIterator.item
}

// 判断是否还能继续读
func (titr *tableIterator) Valid() bool {
	return titr.err != io.EOF
}

// 调整到整个sstable的第一个block的第一个entry
func (titr *tableIterator) seekToFirst() {
	numBlocks := len(titr.table.sst.GetIndexs().Offsets)
	if numBlocks == 0 {
		titr.err = io.EOF
		return
	}
	titr.blockPos = 0
	block, err := titr.table.block(titr.blockPos)
	if err != nil {
		titr.err = err
		return
	}
	titr.blockIterator.tableID = titr.table.fid
	titr.blockIterator.blockID = titr.blockPos
	titr.blockIterator.setBlcok(block)
	titr.blockIterator.seekToFirst()
	titr.item = titr.blockIterator.GetItem()
	titr.err = titr.blockIterator.Error()
}

// 跳转到最后一个block的最后一个entry
func (titr *tableIterator) seekToLast() {
	numBlocks := len(titr.table.sst.GetIndexs().Offsets)
	if numBlocks == 0 {
		titr.err = io.EOF
		return
	}
	titr.blockPos = numBlocks - 1
	block, err := titr.table.block(titr.blockPos)
	if err != nil {
		titr.err = err
		return
	}
	titr.blockIterator.tableID = titr.table.fid
	titr.blockIterator.blockID = titr.blockPos
	titr.blockIterator.setBlcok(block)
	titr.blockIterator.seekToLast()
	titr.item = titr.blockIterator.GetItem()
	titr.err = titr.blockIterator.Error()
}

// 跳转到指定block的指定key，如果key不存在，会跳转到最接近的entryKey>=key
func (titr *tableIterator) seekIdx(idx int, key []byte) {
	titr.blockPos = idx
	block, err := titr.table.block(idx)
	if err != nil {
		titr.err = err
		return
	}
	titr.blockIterator.tableID = titr.table.fid
	titr.blockIterator.blockID = titr.blockPos
	titr.blockIterator.setBlcok(block)
	titr.blockIterator.seek(key)
	titr.item = titr.blockIterator.GetItem()
	titr.err = titr.blockIterator.Error()
}

// 通过二分法搜索offsets
// 如果idx == 0，说明key只能在第一个block中 block[0].MinKey <= key
func (titr *tableIterator) Seek(key []byte) {
	var blockOffset pb.BlockOffset

	/*
		返回一个baseKey刚刚大于key的block
		+---------------------------------------------------+
		|           block1            |       block2        |
		| basekey1           key      | basekey2			|
		+---------------------------------------------------+
		会返回 idx == 2；
		如果key不存在会返回 ind == len-1；
	*/
	idx := sort.Search(len(titr.table.sst.GetIndexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!titr.table.offsets(&blockOffset, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(titr.table.sst.GetIndexs().GetOffsets()) {
			// 如果越界了返回最后一个
			return true
		}
		// 找到一个刚刚大于key的block
		return utils.CompareKeys(blockOffset.GetKey(), key) > 0
	})
	// 如果返回0，说明blocks[1].minkey  > key，只有可能在第0个
	// 或者block[0],minkey > key，不存在该key，所以直接在第0个block中找
	if idx == 0 {
		titr.seekIdx(0, key)
		return
	}
	titr.seekIdx(idx-1, key)

}

// 从头开始(在openTable中传入的option.isAsc == false，所以会seek到Last，这样maxkey也就是item.entry.key)
func (titr *tableIterator) Rewind() {
	if titr.opt.IsAsc {
		titr.seekToFirst()
	} else {
		titr.seekToLast()
	}
}
func (titr *tableIterator) Item() utils.Item {
	return titr.item
}
func (titr *tableIterator) Close() error {
	titr.blockIterator.Close()
	return titr.table.DecrRef()
}

// ConcatIterator 会将tables数组链接为一个迭代器，也就是对tableIter
type ConcatIterator struct {
	Idx     int              // 当前正在使用第几个Iterator
	cur     utils.Iterator   // 当前正在使用的iterator
	iters   []utils.Iterator // iterator数组，对应的tables
	tables  []*table         // 升序的tables
	Options *utils.Options
}

// 根据tables和options创建 ConcatIterator
func NewConcatIterator(ts []*table, opt *utils.Options) *ConcatIterator {
	iters := make([]utils.Iterator, len(ts))
	return &ConcatIterator{
		Idx:     -1, // 还没有初始化
		iters:   iters,
		tables:  ts,
		Options: opt,
	}
}

// 将idx移动到指定的位置，并调整curIter，如果curIter不存在会创建新的
func (ci *ConcatIterator) setIdx(idx int) {
	ci.Idx = idx
	if idx < 0 || idx > len(ci.tables) {
		ci.cur = nil
		return
	}
	if ci.iters[idx] == nil {
		ci.iters[idx] = ci.tables[idx].NewIterator(ci.Options)
	}
	ci.cur = ci.iters[ci.Idx]
}

// 从头开始
func (ci ConcatIterator) Rewind() {
	if len(ci.iters) == 0 {
		return
	}
	if !ci.Options.IsAsc {
		ci.setIdx(0)
	} else {
		ci.setIdx(len(ci.iters) - 1)
	}
	ci.cur.Rewind()
}

// 判断是否有效
func (ci *ConcatIterator) Valid() bool {
	// 注意要先判断是否为nil
	return ci.cur != nil && ci.cur.Valid()
}

// Item
func (ci *ConcatIterator) Item() utils.Item {
	return ci.cur.Item()
}

// if reversed == false Seek到element >=key ;else Seek 到 <=key;
func (ci *ConcatIterator) Seek(key []byte) {
	var idx int
	if ci.Options.IsAsc { // 如果是升序的
		idx = sort.Search(len(ci.tables), func(i int) bool {
			// 小 --> 大
			// [minkey1  maxkey1] , [minkey2  key   maxkey2] , [minkey3  maxkey3]
			return utils.CompareKeys(ci.tables[i].sst.GetMaxKey(), key) >= 0
		})
	} else {
		n := len(ci.tables) - 1
		// 找到一个element.minKey <= key，且idx最小
		idx = n - sort.Search(n+1, func(i int) bool {
			// 大 --> 小
			// [minkey3  maxkey3] , [minkey2  key   maxkey2] ， [minkey1  maxkey1]
			return utils.CompareKeys(ci.tables[n-i].sst.GetMinKey(), key) <= 0
		})
	}
	if idx >= len(ci.tables) || idx < 0 {
		ci.setIdx(-1)
		return
	}

	ci.setIdx(idx)
	ci.cur.Seek(key)
}

// Next
func (ci *ConcatIterator) Next() {
	ci.cur.Next()
	if ci.cur.Valid() {
		return
	}
	for {
		if !ci.Options.IsAsc { // 降序 +1
			ci.setIdx(ci.Idx + 1)
		} else {
			ci.setIdx(ci.Idx - 1)
		}
		if ci.cur == nil {
			return
		}
		ci.cur.Rewind()
		if ci.cur.Valid() {
			break
		}
	}
}
