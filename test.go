package main

import "fmt"

func main() {
	buf := make([]byte, 25)
	c := copy(buf[0:], []byte{1, 2, 3})
	fmt.Printf("c: %v\n", c)
	fmt.Printf("(c == len(buf)): %v\n", (c == len(buf)))
}
