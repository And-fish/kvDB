package main

import (
	"fmt"
	"sort"
)

func main() {
	a := make([][]uint64, 3)
	a = [][]uint64{[]uint64{1, 2}, []uint64{5, 9}, []uint64{3, 7}, []uint64{3, 3}}
	sort.Slice(a, func(i, j int) bool {
		return a[i][0] < a[j][0] || ((a[i][0] == a[j][0]) && (a[i][1] < a[j][1]))
	})

	fmt.Printf("a: %v\n", a)

}
