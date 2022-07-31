// 对syscall的封装
package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

/*
MREMAP_MAYMOVE:

	By default, if there is not sufficient space to expand a mapping at its current location, then mremap() fails.
	If this flag is specified, then the kernel is permitted to relocate the mapping to a new virtual address, if necessary.
	If the mapping is relocated, then absolute pointers into the old mapping location become invalid (offsets relative to the starting address of the mapping should be employed).

	mman.h: #define MREMAP_MAYMOVE		1
*/
const MREMAP_MAYMOVE = 0x1

// 封装mmap，将文件映射到用户态内存中，可以直接在返回的[]byte上使用
//
//	void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	prot := unix.PROT_READ
	if writable {
		prot |= unix.PROT_WRITE
	}
	// 调用unix.Mmap，指定fid、size、从文件头开始、权限和flag为MAP_SHARED(内存数据同步到磁盘)
	return unix.Mmap(int(fd.Fd()), 0, int(size), prot, unix.MAP_SHARED)
}

// 封装mremap，重新将文件映射到一块用户态内存中，等同于 munmap + mmap
// void *mremap(void *old_address, size_t old_size,size_t new_size, int flags, ... /* void *new_address */);
func mremap(data []byte, size int) ([]byte, error) {
	// 将data作为通过slice重构
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))

	mmapAddr, _, err := unix.Syscall6(unix.SYS_MREMAP,
		header.Data,             // void *old_address
		uintptr(header.Len),     // size_t old_size
		uintptr(size),           // size_t new_size
		uintptr(MREMAP_MAYMOVE), // int flags
		0, 0,
	)
	if err != 0 {
		return nil, err
	}
	header.Data = mmapAddr
	header.Cap = size
	header.Len = size
	return data, nil
}

// 封装munmap，用于解除映射关系
// int munmap(void *addr, size_t length);
func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, err := unix.Syscall(unix.SYS_MUNMAP,
		uintptr((unsafe.Pointer(&data[0]))),
		uintptr((len(data))),
		0,
	)
	if err != 0 {
		return err
	}
	return nil
}

// 封装madvise，可以用于配合mmap做一个预读操作，避免性能抖动
// int madvise(void *addr, size_t length, int advice);
func madvise(buf []byte, readahead bool) error {
	// 默认参数，预读前15个页和后16个页
	flag := unix.MADV_NORMAL
	// 如果不需要预读
	if !readahead {
		flag = unix.MADV_RANDOM
	}
	return unix.Madvise(buf, flag)
}

// 封装msync，将映射到内存中的数据直接写入到磁盘中
// int msync(void *addr, size_t length, int flags);
func msync(buf []byte) error {
	// MS_SYNC请求写入，等到写入完成后再返回
	return unix.Msync(buf, unix.MAP_SYNC)
}
