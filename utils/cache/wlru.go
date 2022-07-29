package cache

import (
	"container/list"
	"fmt"
)

// windowLRU中需要的数据结构
type windowLRU struct {
	// 数据实际存储的位置，其中索引为key的hash值，值是*list.Element
	data map[uint64]*list.Element
	// 容量上限
	cap int
	// 用于lru淘汰策略
	list *list.List
}

// list.Element.Value中存储的实际的数据
type storeItem struct {
	// 表示现在位于哪个阶段
	// stage == 0 ：windowLRU
	// stage == 1 ： segmentedLRU A1 probation
	// stage == 2 ： segmentedLRU A2 protected
	stage int
	// 经过hash函数处理后的keyHash
	key uint64
	// 用于校验
	conflict uint64
	// 实际的value
	value interface{}
}

// 向Window-LRU中添加storeItem类型的数据，返回的是被淘汰掉的 storeItem 和 触发了淘汰策略
func (wl *windowLRU) add(newItem storeItem) (eItem storeItem, evicted bool) {
	// 如果LRU没有满，直接插入
	if wl.list.Len() < wl.cap {
		// 头插法
		wl.data[newItem.key] = wl.list.PushFront(&newItem)
		// 返回空的storeItem，和false
		return storeItem{}, false
	}

	// 如果已经满了，按照LRU淘汰策略会直接从尾部淘汰
	element := wl.list.Back()
	// 利用类型断言将interface的Value转化为storeItem类型
	item := element.Value.(*storeItem)
	// 从data map中删除数据
	delete(wl.data, item.key)

	// 不需要重新新建变量，直接对链表中的数据赋值
	// 将被淘汰的item赋值给eItem，将list中的item值赋值为newItem；
	eItem, *item = *item, newItem
	// 将新的key-value放到Data中，key是newItem的key，element没有被修改，修改的是element.value指向的storeItem的值
	wl.data[item.key] = element
	// 将这个Element移动到list的头
	wl.list.MoveToFront(element)

	// 返回被淘汰掉的item，和true
	return eItem, true
}

// 因为实际的返回值的操作会在外层封装，所以对于WLRU只需要调节WindowLRU的顺序
func (wl *windowLRU) get(element *list.Element) {
	// 将被访问的element移动到list的头
	wl.list.MoveToFront(element)
}

// 创建一个新的WLRU
func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

// 测试用
func (wl *windowLRU) String() string {
	var res string
	for e := wl.list.Front(); e != nil; e = e.Next() {
		res += fmt.Sprintf("%v", e.Value.(*storeItem).value)
	}
	return res
}
