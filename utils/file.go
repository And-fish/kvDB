package utils

import (
	"hash/crc32"
	"path"
	"strconv"
	"strings"
)

// 根据fileName获取到FID
func FID(fileName string) uint64 {
	// 将路径提取为文件的名字，也就是路径的最后一个元素
	fileName = path.Base(fileName)
	// 判断后缀是否是".sst"，也就是判断是不是sst文件
	if !strings.HasSuffix(fileName, ".sst") {
		return 0
	}
	// 去掉文件后缀
	fileName = strings.TrimSuffix(fileName, ".sst")
	id, err := strconv.Atoi(fileName)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// 计算checksum
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
