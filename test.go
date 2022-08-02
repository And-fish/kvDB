package main

import (
	"fmt"
	"io/ioutil"
)

func main() {
	fileInfo, _ := ioutil.ReadDir(".")
	fmt.Printf("fileInfo: %v\n", fileInfo[2].Name())
}
