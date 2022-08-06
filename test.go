// package main

// import (
// 	"bytes"
// 	"fmt"
// 	"io"
// )

// type h struct {
// 	ids []uint32
// }

// func main() {
// 	var buf bytes.Buffer
// 	buf.Write([]byte("helllo"))

// 	data := buf.Bytes()
// 	buf2, err := buf.ReadByte()
// 	fmt.Println(err == io.EOF)
// 	fmt.Println(buf2)
// 	fmt.Println(data)

// }
