// 对外暴露的mmap类
package mmap

import "os"

func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

func Munmap(data []byte) error {
	return munmap(data)
}

func Madvise(buf []byte, readahead bool) error {
	return madvise(buf, readahead)
}

func Msync(buf []byte) error {
	return msync(buf)
}

func Mremap(data []byte, size int) ([]byte, error) {
	return mremap(data, size)
}
