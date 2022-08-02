package utils

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"

	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"
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

// 校验checksum
func VerifyChecksum(data []byte, expected []byte) error {
	trueChecksum := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := Bytes2Uint64(expected)
	if trueChecksum != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", trueChecksum, expectedU64)
	}
	return nil
}

func openDir(dir string) (*os.File, error) {
	return os.Open(dir)
}

// 在创建或删除文件时，必须确保文件的目录条目是同步的，以便确保文件是可见的(如果系统崩溃)。
/*
  Etcd的例子：
	For the 4th operation (rename of wal.tmp to wal) to be persisted to disk, the parent directory has to be fsync'd.
	If not, a crash just after acknowledging the user can result in a data loss.
	Specifically, the rename can be reordered on some file systems, thus not issued immediately by the fs.
	In such a case, on recovery, the server would see the file wal.tmp but not wal.
	On seeing this, I believe etcd just unlinks the tmp file and therefore can lose the user data.
	If this happens on two nodes on a three node cluster, then a global data loss is possible.
	We have reproduced this particular data loss issue using our testing framework.
	As a fix, it would be safe to fsync the parent directory on creat or rename of files.
*/
func SyncDir(dir string) error {
	file, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	if err := file.Sync(); err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	closeErr := file.Close()
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// 生成sst文件名
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

// 获取当前WorkDir下面的所有sstable ID
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfo, err := ioutil.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, file := range fileInfo {
		if file.IsDir() {
			continue
		}
		fid := FID(file.Name())
		if fid != 0 {
			idMap[fid] = struct{}{}
		}
	}
	return idMap
}
