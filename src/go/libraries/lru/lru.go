package lru

import (
	"container/list"
	"fmt"
	"sync"
)

const maxItems = 10

type CacheItem struct {
	key   string
	value []byte
}

type LRUCache struct {
	items           map[string]*list.Element
	queue           *list.List
	capacity        int
	maxItemSizeInMB int
	currentSizeInMB int
	mutex           sync.RWMutex
}

func NewLRUCache(maxItemSizeInMB int) *LRUCache {
	return &LRUCache{
		items:           make(map[string]*list.Element),
		queue:           list.New(),
		capacity:        maxItems,
		maxItemSizeInMB: maxItemSizeInMB,
		currentSizeInMB: 0,
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, found := c.items[key]; found {
		c.queue.MoveToFront(element)
		return element.Value.(*CacheItem).value, true
	}
	return nil, false
}

func (c *LRUCache) Set(key string, value []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	itemSizeInMB := len(value) / (1024 * 1024)
	if itemSizeInMB > c.maxItemSizeInMB {
		return fmt.Errorf("item size (%d MB) exceeds maximum allowed size (%d MB)", itemSizeInMB, c.maxItemSizeInMB)
	}

	if element, found := c.items[key]; found {
		c.queue.MoveToFront(element)
		oldItem := element.Value.(*CacheItem)
		c.currentSizeInMB -= len(oldItem.value) / (1024 * 1024)
		oldItem.value = value
		c.currentSizeInMB += itemSizeInMB
		return nil
	}

	for c.queue.Len() >= c.capacity || c.currentSizeInMB+itemSizeInMB > c.capacity*c.maxItemSizeInMB {
		oldest := c.queue.Back()
		if oldest == nil {
			break
		}
		c.queue.Remove(oldest)
		oldestItem := oldest.Value.(*CacheItem)
		delete(c.items, oldestItem.key)
		c.currentSizeInMB -= len(oldestItem.value) / (1024 * 1024)
	}

	item := &CacheItem{key: key, value: value}
	element := c.queue.PushFront(item)
	c.items[key] = element
	c.currentSizeInMB += itemSizeInMB
	return nil
}

func (c *LRUCache) Remove(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, found := c.items[key]; found {
		c.queue.Remove(element)
		item := element.Value.(*CacheItem)
		c.currentSizeInMB -= len(item.value) / (1024 * 1024)
		delete(c.items, key)
	}
}

func (c *LRUCache) Size() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.queue.Len()
}

func (c *LRUCache) CurrentSizeInMB() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.currentSizeInMB
}
