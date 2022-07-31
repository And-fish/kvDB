package main

import (
	"encoding/binary"
	"fmt"
)

func main() {
	buf := make([]byte, 25)
	binary.BigEndian.PutUint32(buf[0:], 5000)
	binary.PutUvarint(buf[10:], 5000)
	binary.LittleEndian.PutUint32(buf[20:], 5000)
	fmt.Printf("buf: %v\n", buf)
	fmt.Printf("%b\n", 5000) // 00010011 10001000
	fmt.Printf("%b\n", 19)   // 00010011
	fmt.Printf("%b\n", 136)  // 10001000
	fmt.Printf("%b\n", 39)   // 00100111

}
