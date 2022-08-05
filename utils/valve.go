package utils

import (
	"sync"
)

type Valve struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

// 创建Value
func NewValue(max int) *Valve {
	return &Valve{
		ch:    make(chan struct{}, max),
		errCh: make(chan error, max),
	}
}

// 循环监听ch和errCh，如果 <-errCh != nil，会返回err
func (v *Valve) Run() error {
	for {
		select {
		case v.ch <- struct{}{}:
			v.wg.Add(1)
			return nil
		case err := <-v.errCh:
			if err != nil {
				return err
			}
		}
	}
}

// 结束一个value的监听，如果传入的err!=nil 会发送到errCh中通知其他协程
func (v *Valve) Done(err error) {
	if err != nil {
		v.errCh <- err
	}
	select {
	case <-v.ch:
	default:
		panic("Throttle Do Done mismatch")
	}
	v.wg.Done()
}

// 关闭Value，等待所有的协程结束，关闭ch、errCh，返回最后的err
func (v *Valve) Finish() error {
	v.once.Do(
		func() {
			v.wg.Wait()
			close(v.ch)
			close(v.errCh)
			for err := range v.errCh {
				if err != nil {
					v.finishErr = err
					return
				}
			}
		})
	return v.finishErr
}
