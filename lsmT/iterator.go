package lsmt

import (
	"bytes"
	"fmt"
	"io"
	"kvdb/utils"
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

// // 跳转到某个block
// func (bitr *blockIterator) seek(key []byte) {
// 	bitr.err = nil
// 	startIndex := 0
// 	sort.Search(len(bitr.entryOffset), func(idx int) bool {
// 		if idx < startIndex {
// 			return false
// 		}
// 		bitr .se
// 	})
// }
