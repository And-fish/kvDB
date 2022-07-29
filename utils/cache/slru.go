package cache

import (
	"container/list"
	"fmt"
)

// 两个状态，分别对应 probation A1 和 protected A2两种状态
const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

// segmentedLRU的数据结构，其中A1状态是缓冲区，A2状态是较为安全的状态
type segmentedLRU struct {
	// 两个状态会共用一个map
	data map[uint64]*list.Element
	// 两个段的大小也会不一样
	a1Cap, a2Cap int
	// 各自维护一个淘汰策略
	a1, a2 *list.List
}

// 计算SLRU使用了长度
func (sl *segmentedLRU) Len() int {
	return sl.a1.Len() + sl.a2.Len()
}

// 将一个storeItem插入到SLRU中，不会有返回值，因为如果在SLRU中淘汰了会被彻底淘汰
func (sl *segmentedLRU) add(newItem storeItem) {
	// 所有加入SLRU中的数据都会被添加到A1中
	newItem.stage = STAGE_ONE

	// 如果A1没满，且整个SLRU区域也没有满，会将item直接插入到A1中
	if sl.a1.Len() < sl.a1Cap && sl.Len() < sl.a1Cap+sl.a2Cap {
		// 将newItem用头插法放到A1中
		element := sl.a1.PushFront(&newItem)
		// 再对data map赋值
		sl.data[newItem.key] = element
		return
	}

	// 如果A1满了，或者SLRU满了
	// 淘汰掉最后的element对应的item
	element := sl.a1.Back()
	item := element.Value.(*storeItem)

	// 真正的从cache中删除
	delete(sl.data, item.key)

	// 将newItem赋值给element指向的item
	*item = newItem
	// 再data map中插入数据
	sl.data[item.key] = element
	// 将对应的element移动到A1的最前面
	sl.a1.MoveToFront(element)
}

// 当对一个element访问时，SLRU需要的逻辑，和WLRU一样在外层做了返回值的封装，所以不需要返回
func (sl *segmentedLRU) get(element *list.Element) {
	// 获取到element对应的storeItem
	item := element.Value.(*storeItem)

	// 如果访问的数据已经在A2中了，直接将它调整到A2头部就行
	if item.stage == STAGE_TWO {
		sl.a2.MoveToFront(element)
		return
	}

	// 如果在A1中，通过判断A2是否满了，尝试直接放入到A2中
	// 如果可以直接放入
	if sl.a2.Len() < sl.a2Cap {
		// 将element在A1中删除
		sl.a1.Remove(element)
		// 将对应的storeItem.stage调整为A2状态
		item.stage = STAGE_TWO
		// 将item插入到A2的头部，并更新data中的element
		sl.data[item.key] = sl.a2.PushFront(item)
		return
	}

	// 如果不能直接放入A2，就需要从A2中淘汰数据到A1中；
	a2Back := sl.a2.Back()
	a2Item := a2Back.Value.(*storeItem)

	// 将A2中element对应的storeItem 和 A1中element对应的storeItem 交换，并修改对应的stage
	*a2Item, *item = *item, *a2Item
	a2Item.stage = STAGE_TWO
	item.stage = STAGE_ONE

	// 修改data map中的数据
	sl.data[item.key] = element
	sl.data[a2Item.key] = a2Back

	// 将两个element都放到对应的list的头节点
	sl.a1.MoveToFront(element)
	sl.a2.MoveToFront(a2Back)
}

// 用于当windowLRU中满了需要从SLRU中挑选出一个数据来做对比
func (sl *segmentedLRU) victim() *storeItem {
	// 如果SLRU没有满就不需要从A1中淘汰
	if sl.Len() < sl.a1Cap+sl.a2Cap {
		return nil
	}
	// 否则从A1中淘汰一个元素(list尾)
	return sl.a1.Back().Value.(*storeItem)
}

// 新建SLRU
func newSLRU(data map[uint64]*list.Element, a1Cap, a2Cap int) *segmentedLRU {
	return &segmentedLRU{
		data:  data,
		a1Cap: a1Cap,
		a2Cap: a2Cap,
		a1:    list.New(),
		a2:    list.New(),
	}
}

// 用于test测试
func (sl *segmentedLRU) String() string {
	var res string
	for e := sl.a2.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	res += fmt.Sprintf(" | ")
	for e := sl.a1.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	return res
}
