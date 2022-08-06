package utils

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"unsafe"
)

const maxWalHanderSize = int(unsafe.Sizeof(uint32(0))) + 1 +
	int(unsafe.Sizeof(uint32(0))) + 1 +
	int(unsafe.Sizeof(byte(0))) + 1 +
	int(unsafe.Sizeof(uint64(0))) + 1

type LogEntry func(entry *Entry, vp *ValuePtr) error

type WalHander struct {
	KeyLen   uint32
	ValueLen uint32
	Meta     byte
	TTL      uint64
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int // 读的Byte的数量
}

// 实现Redaer接口
func (hr *HashReader) Read(buf []byte) (int, error) {
	n, err := hr.R.Read(buf)
	if err != nil {
		return n, err
	}
	hr.BytesRead += n
	return hr.H.Write(buf[:n])
}

// 实现ByteRaeder接口
func (hr *HashReader) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	_, err := hr.Read(buf)
	return buf[0], err
}

// 加码
func (wh *WalHander) Encode(buf []byte) int {
	index := 0
	index = binary.PutUvarint(buf[index:], uint64(wh.KeyLen))
	index += binary.PutUvarint(buf[index:], uint64(wh.ValueLen))
	index += binary.PutUvarint(buf[index:], uint64(wh.Meta))
	index += binary.PutUvarint(buf[index:], uint64(wh.TTL))
	return index
}

// 将reader中的数据读取出来，解码为WalHander
func (wh *WalHander) Decode(reader *HashReader) (int, error) {
	var err error
	keyLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	wh.KeyLen = uint32(keyLen)

	valueLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	wh.ValueLen = uint32(valueLen)

	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	wh.Meta = byte(meta)

	wh.TTL, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// 将entry按照WalHander + key +value +crc32格式写入到指定的buf中，返回写入的长度
func WalCodec(buf *bytes.Buffer, entry *Entry) int {
	buf.Reset()
	wh := WalHander{
		KeyLen:   uint32(len(entry.Key)),
		ValueLen: uint32(len(entry.Value)),
		TTL:      entry.TTL,
	}

	hash := crc32.New(CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode
	var headerEncode [maxWalHanderSize]byte
	size := wh.Encode(headerEncode[:])
	Panic2(writer.Write(headerEncode[:size]))
	Panic2(writer.Write(entry.Key))
	Panic2(writer.Write(entry.Value))

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	Panic2(buf.Write(crcBuf[:]))

	/*

		+-----------------------------+
		| header | key |value | crc32 |
		+-----------------------------+
	*/

	return len(headerEncode[:size]) + len(entry.Key) + len(entry.Value) + len(crcBuf)
}

// 预估当前entry在wal文件中的size
func EstimateWalCodecSize(entry *Entry) int {
	return len(entry.Key) + len(entry.Value) + 8 + crc32.Size + maxWalHanderSize
}

// 初始化HashReader
func NewHashReader(reader io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: reader,
		H: hash,
	}
}

func (hr *HashReader) Sum32() uint32 {
	return hr.H.Sum32()
}

func (entry *Entry) IsZero() bool {
	return len(entry.Key) == 0
}
func (entry *Entry) LogHeaderLen() int {
	return entry.Hlen
}
func (entry *Entry) LogOffset() uint32 {
	return entry.Offset
}
