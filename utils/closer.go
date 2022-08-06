package utils

import "sync"

type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

// 创建Closer
func NewCloser() *Closer {
	closer := &Closer{
		waiting: sync.WaitGroup{},
	}
	closer.CloseSignal = make(chan struct{})
	return closer
}

// Close用于上游通知下游携程进行资源回收，并等待携程通知回收完毕
func (c *Closer) Close() {
	// 关闭通讯channel
	close(c.CloseSignal)
	// 等待携程结束
	c.waiting.Wait()
}

// Done用于下游携程通知上游携程回收完毕，可以正式关闭
func (c *Closer) Done() {
	// 减一
	c.waiting.Done()
}

// Add用于表示需要等待的下游协程+n
func (c *Closer) Add(n int) {
	// 加n
	c.waiting.Add(n)
}
