package lsmt

type levelManager struct {
	// 已经分配出去的最大fid，只要创建了memtable就算已经分配
	maxFID uint64
	opt    *Options
	cache  *cache
}
