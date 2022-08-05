package lsmt

import afCache "kvdb/utils/cache"

// LSMcache，是utils/cache的调用
type cache struct {
	indexs *afCache.Cache
	blocks *afCache.Cache
}

type blockBuffer struct {
	b []byte
}

// 默认cache大小
const defaultCacheSize = 1024

// 创建新cache
func newCach(opt *Options) *cache {
	return &cache{
		indexs: afCache.NewCache(defaultCacheSize),
		blocks: afCache.NewCache(defaultCacheSize),
	}
}

// close
func (c *cache) close() error {
	return nil
}

// 插入
func (c *cache) addIndex(fid string, t *table) {
	// fid作为key，table作为value
	c.indexs.Set(fid, t)
}
