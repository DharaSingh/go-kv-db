package go_kv_db

import (
	"fmt"
	"sync"
	"time"
)

type Cache struct {
	items         map[string]Item
	currentMemory uint64 // current memory stats for entire cache
	maxMemory     uint64 // total memory available for cache
	sync.Mutex
}

func NewCache(capacity int, maxMemory uint64) (*Cache, error) {
	cache := &Cache{
		items:     make(map[string]Item, capacity), // preallocate map with capacity
		maxMemory: maxMemory,
	}
	return cache, nil
}

func (c *Cache) Set(key string, value interface{}, ttl int) error {
	c.Lock()
	defer c.Unlock()

	itemSize := uint64(len(key)) + uint64(len(fmt.Sprintf("%v", value))) + uint64(8) // with TTL always set
	exp := int64(0)
	if ttl > 0 {
		exp = time.Now().Add(time.Duration(ttl) * time.Second).UnixNano()
	}

	if c.currentMemory+itemSize > c.maxMemory {
		return fmt.Errorf("memory limit exceeded")
	}

	c.items[key] = Item{
		value:      value,
		expiration: exp,
	}
	c.currentMemory += itemSize
	return nil
}

func (c *Cache) Get(key string) (interface{}, bool) {
	c.Lock()
	item, ok := c.items[key]
	c.Unlock()
	if !ok {
		return nil, false
	}

	if item.expiration < time.Now().UnixNano() {
		c.Delete(key)
		return nil, false
	}

	return item.value, true
}

func (c *Cache) Delete(key string) {
	c.Lock()
	defer c.Unlock()

	item, exists := c.items[key]
	if exists {
		itemSize := uint64(len(key)) + uint64(len(fmt.Sprintf("%v", item.value))) + uint64(8) // int64 size TTL
		delete(c.items, key)
		c.currentMemory -= itemSize
	}
}

func (c *Cache) Stats() (uint64, int) {
	c.Lock()
	defer c.Unlock()

	return c.currentMemory, len(c.items)
}
