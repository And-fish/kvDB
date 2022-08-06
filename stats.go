package kv

import "kvdb/utils"

type Stats struct {
	closer   *utils.Closer
	EntryNum int64
}

func (s *Stats) close() error {
	return nil
}

func (s *Stats) StartStats() {
	defer s.closer.Done()
	for {
		select {
		case <-s.closer.CloseSignal:
			return
		}
	}
}

func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser()
	s.EntryNum = 1
	return s
}
