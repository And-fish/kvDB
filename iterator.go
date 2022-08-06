package kv

import (
	lsmt "kvdb/lsmT"
	"kvdb/utils"
)

type DBIterator struct {
	itr  utils.Iterator
	vlog *valueLog
}

type Item struct {
	entry *utils.Entry
}

func (iter *DBIterator) Close() error {
	return iter.itr.Close()
}

func (i *Item) Entry() *utils.Entry {
	return i.entry
}

func (iter *DBIterator) Valid() bool {
	return iter.itr.Valid()
}

func (iter *DBIterator) Item() utils.Item {
	entry := iter.itr.Item().Entry()
	var value []byte

	if entry != nil && utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		vp.Decode(entry.Value)
		valBuf, unlockCallBack, err := iter.vlog.read(&vp)
		defer utils.RunCallback(unlockCallBack)

		if err != nil {
			return nil
		}
		value = utils.SafeCopy(nil, valBuf)
	}
	if entry.IsDeletedOrExpired() || value == nil {
		return nil
	}

	res := &utils.Entry{
		Key:          entry.Key,
		Value:        value,
		TTL:          entry.TTL,
		Meta:         entry.Meta,
		Version:      entry.Version,
		Offset:       entry.Offset,
		Hlen:         entry.Hlen,
		ValThreshold: entry.ValThreshold,
	}
	return res
}

// 实现接口
func (iter *DBIterator) Seek(key []byte) {
}

func (iter *DBIterator) Rewind() {
	iter.itr.Rewind()
	for ; iter.Valid() && iter.Item() == nil; iter.itr.Next() {

	}
}

func (iter *DBIterator) Next() {
	iter.itr.Next()
	for ; iter.Valid() && iter.Item() == nil; iter.itr.Next() {

	}
}

func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	iters := make([]utils.Iterator, 0)
	iters = append(iters, db.lsm.NewIterators(opt)...)

	res := &DBIterator{
		itr:  lsmt.NewMergeIterator(iters, opt.IsAsc),
		vlog: db.vlog,
	}
	return res
}
