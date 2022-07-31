package lsmt

import "sync"

type compactStatus struct {
	sync.RWMutex
}
