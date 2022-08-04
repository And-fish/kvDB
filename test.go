package main

import (
	"fmt"
	"sort"
)

func main() {
	// b := []uint{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	c := []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	x := 9 - sort.Search(10, func(i int) bool {
		return c[9-i] <= 9
	})
	fmt.Println(c[x])
}
