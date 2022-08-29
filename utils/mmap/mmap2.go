// 对外暴露的mmap类
package mmap

import "os"

// mmap将文件(size大小)映射到内存中
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// munmap取消data在内存中的映射
func Munmap(data []byte) error {
	return munmap(data)
}

// 对buf预读
func Madvise(buf []byte, readahead bool) error {
	return madvise(buf, readahead)
}

// 将buf sync到磁盘中
func Msync(buf []byte) error {
	return msync(buf)
}

// data重新映射到内存中
func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}
