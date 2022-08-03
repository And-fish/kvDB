package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"kvdb/pb"
	"kvdb/utils"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *Options
	file                      *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels    []levelManifest
	Tables    map[uint64]TableManifest
	Creations int
	Deletions int
}

// TableManifest 包含sst的基本层级信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}
type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

// TableMeta sst 的一些元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

// 读取manifestFile
type readerBuf struct {
	reader *bufio.Reader
	count  int64
}

// 实现Reder接口
func (rb *readerBuf) Read(buf []byte) (n int, err error) {
	n, err = rb.reader.Read(buf)
	rb.count += int64(n)
	return
}

// 创建一个manifest
func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

// 创建ManifestChange
func newCreateChange(tableID uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       tableID,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

// 获取manifest的[]Cahnge
func (manifest *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(manifest.Tables))
	for tableID, tableManifest := range manifest.Tables {
		changes = append(changes, newCreateChange(tableID, int(tableManifest.Level), tableManifest.Checksum))
	}
	return changes
}

// 将manifestChange写入manifest
func applyManifestChange(manifest *Manifest, change *pb.ManifestChange) error {
	switch change.Op {
	case pb.ManifestChange_CREATE:
		// 该Change应该是没有被插入过的
		if _, ok := manifest.Tables[change.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", change.Id)
		}
		// tables是表示某一个table的元数据
		manifest.Tables[change.Id] = TableManifest{
			Level:    uint8(change.Level),                  //该 table在哪一层
			Checksum: append([]byte{}, change.Checksum...), //该table的cheucksum
		}
		// 创建足够的层级，level表示每一层有哪些tables
		for len(manifest.Levels) <= int(change.Level) {
			manifest.Levels = append(manifest.Levels, levelManifest{make(map[uint64]struct{})})
		}
		// 在该Change对应层下面记录上table
		manifest.Levels[change.Level].Tables[change.Id] = struct{}{}
		manifest.Creations++ // ++

	// 删除指令
	case pb.ManifestChange_DELETE:
		tableManifest, ok := manifest.Tables[change.Id]
		if !ok { // 要被删除的table应该是要已经存在的
			return fmt.Errorf("MANIFEST removes non-existing table %d", change.Id)
		}
		delete(manifest.Levels[tableManifest.Level].Tables, change.Id) // Delete指令没有checksum和level信息
		delete(manifest.Tables, change.Id)
		manifest.Deletions++

	// 未知错误
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

// 根据maifestChangeSet构建Manifest
func applyChangeSet(manifest *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(manifest, change); err != nil {
			return err
		}
	}
	return nil
}

// 根据manifest覆写文件
func helpRewrite(dir string, manifest *Manifest) (*os.File, int, error) {
	path := filepath.Join(dir, utils.ManifestRewriteFilename)
	file, err := os.OpenFile(path, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	numCreations := len(manifest.Tables)
	changes := manifest.asChanges() // 如果manifest是空的，changes就是空的[]
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		file.Close()
		return nil, 0, err
	}

	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))                              // 0，if manifest{}
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable)) // 0，if manifest{}
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	/*
		buf: 外 ---> 内
		+-----------------------------------------------------------+
		| MagicText[:] | MagicVersion | changeSetBuf_len | checksum |
		+-----------------------------------------------------------+
	*/
	// 写入buf
	if _, err := file.Write(buf); err != nil {
		file.Close()
		return nil, 0, err
	}
	// 写回磁盘
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, 0, err
	}

	// Window的rename必须要在文件close状态才能执行
	if err := file.Close(); err != nil {
		return nil, 0, err
	}

	// Window程序必须要将文件Close才能rename(试一试就知道了hah)
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(path, manifestPath); err != nil {
		return nil, 0, err
	}
	file, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, 0, err
	}
	// 将目录也需要同步一下
	if err := utils.SyncDir(dir); err != nil {
		file.Close()
		return nil, 0, err
	}

	return file, numCreations, nil
}

// ReplayManifestFile 可以对存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(file *os.File) (manifest *Manifest, truncOffset int64, err error) {
	var magicBuf [8]byte

	reader := &readerBuf{
		reader: bufio.NewReader(file),
	}
	// 读8byte的MagicText / MagicVersion
	if _, err := io.ReadFull(reader, magicBuf[:]); err != nil {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	// 判断是否MagicText正确
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return &Manifest{}, 0, utils.ErrBadMagic
	}
	// 判断MagicVersion
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != utils.MagicVersion {
		return &Manifest{}, 0, fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, utils.MagicVersion)
	}

	build := createManifest()
	// 循环读
	for {
		truncOffset = reader.count
		var lenCrcBuf [8]byte
		// change_len + checksum
		_, err = io.ReadFull(reader, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF { // 读完了就break
				break
			}
			return &Manifest{}, 0, err
		}
		lenght := binary.BigEndian.Uint32(lenCrcBuf[0:4])

		// changebuf
		var buf = make([]byte, lenght)
		if _, err := io.ReadFull(reader, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return &Manifest{}, 0, err
		}
		// 校验checksum
		if crc32.Checksum(buf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return &Manifest{}, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return &Manifest{}, 0, err
		}
		if err := applyChangeSet(build, &changeSet); err != nil {
			return &Manifest{}, 0, err
		}
	}

	return build, truncOffset, err
}

// OpenManifest 打开manifestFile
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{
		opt:  opt,
		lock: sync.Mutex{},
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	// 如果打开失败
	if err != nil {
		// 文件存在但是没有权限仍然会报error
		if !os.IsNotExist(err) { // 如果存在就返回false，所以如果文件存在才会执行if
			return mf, err
		}
		// 否则就创建一个新的 manifestFile
		manifest := createManifest()                        // 首先创建一个manifest
		fp, numTable, err := helpRewrite(opt.Dir, manifest) // 根据manifest覆写文件
		utils.CondPanic(numTable == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))
		if err != nil {
			return mf, err
		}
		mf.file = fp
		f = fp
		mf.manifest = manifest
		return mf, nil
	}

	// 如果成功打开，将MANIFEST文件加载到manifest中
	manifest, truncOffset, err := ReplayManifestFile(f)
	if err != nil {
		return mf, err
	}

	// Truncate文件，保证文件不会有一半的数据
	if err = f.Truncate(truncOffset); err != nil {
		_ = f.Close()
		return mf, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return mf, err
	}
	mf.file = f
	mf.manifest = manifest
	return mf, nil
}

// 覆写
func (mf *ManifestFile) rewrite() error {
	// 一样的，windows必须要先close
	if err := mf.file.Close(); err != nil {
		return nil
	}
	// 按照Manifest重写
	fp, newCreations, err := helpRewrite(mf.opt.Dir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = newCreations
	mf.manifest.Deletions = 0
	mf.file = fp
	return nil
}

// 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.file.Close(); err != nil {
		return err
	}
	return nil
}

// 添加Change指令到manifestFile中
func (mf *ManifestFile) addChanges(changes []*pb.ManifestChange) error {
	changeSet := pb.ManifestChangeSet{Changes: changes}
	buf, err := changeSet.Marshal() // Marshal为byte数组
	if err != nil {
		return err
	}
	// 文件加锁
	mf.lock.Lock()
	defer mf.lock.Unlock()
	// 先写入到当前的manifest中
	if err := applyChangeSet(mf.manifest, &changeSet); err != nil {
		return err
	}

	// 如果Delet次数超过了阈值 且 至少能缩小1/10
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil { // 重写MANIFEST
			return err
		}
	} else {
		/*
			MANIFEST文件：
			+-----------------------------------------------------------------------------------------------------------------------+
			| MagicText[:] | MagicVersion | changeSet0Buf_len | checksum0 | changeSet0 | changeSet1Buf_len | checksum1 | changeSet1 | ... |
			+-----------------------------------------------------------------------------------------------------------------------+

			buf:
			+-----------------------------------------+
			| changeSetBuf_len | checksum | changeSet |
			+-----------------------------------------+
		*/
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.file.Write(buf); err != nil {
			return err
		}
	}
	err = mf.file.Sync()
	return err
}

// 对外暴露的Add接口函数
func (mf *ManifestFile) AddChanges(changes []*pb.ManifestChange) error {
	return mf.addChanges(changes)
}

// 添加Create指令到manifestFile中
func (mf *ManifestFile) AddTableMeta(levelNum int, table *TableMeta) error {
	err := mf.addChanges([]*pb.ManifestChange{
		newCreateChange(table.ID, levelNum, table.Checksum),
	})
	if err != nil {
		return err
	}
	return nil
}

// 删除Manifest中没有使用的sst文件
func (mf *ManifestFile) RevertToManifest(mayNeedID map[uint64]struct{}) error {
	// check manifest中的table是否都存在
	for id := range mf.manifest.Tables {
		if _, ok := mayNeedID[id]; !ok {
			return fmt.Errorf("file does not exist for table %d", id)
		}
	}

	// Delete 没有在manifest中记录的table
	for id := range mayNeedID {
		if _, ok := mf.manifest.Tables[id]; !ok {
			utils.Err(fmt.Errorf("Table file %d  not referenced in MANIFEST", id))
			fileName := utils.FileNameSSTable(mf.opt.Dir, id)
			if err := os.Remove(fileName); err != nil {
				return errors.Wrapf(err, "While removing table %d", id)
			}
		}
	}
	return nil
}

// 获取ManifestFile对应的Manifest
func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
