package lsmt

type LSM struct {
	memtable  *memtable
	immutable []*memtable
	levels *levelManager
}
