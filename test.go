package main

import "fmt"

type heloo struct {
	id uint64
}
type Fun func(uint64) error

func (h *heloo) add() func(uint64) error {
	return func(i uint64) error {
		h.id += i
		return nil
	}
}

func (h *heloo) g(f Fun) {
	f(55)
	return
}

func (h *heloo) jia() {
	h.g(h.add())
	return
}
func main() {
	h := heloo{id: 44}
	h.jia()
	fmt.Printf("h: %v\n", h)
}
