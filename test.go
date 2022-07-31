package main

import (
	"fmt"
	"sort"
)

func main() {
	buf := [10]uint32{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}
	i := sort.Search(len(buf)-1, func(i int) bool {
		return buf[i] > 17
	})
	fmt.Println(buf[i], i)
}
