package go_kv_db

import (
	"runtime"
	"sync"
	"time"
)

type CacheSharded struct {
	shards   []*Shard
	channels []chan *itemOperation
	deleteCh chan string
}

type Shard struct {
	items map[string]Item
	sync.RWMutex
}

type itemOperation struct {
	key  string
	item *Item
}

func NewCacheSharded(capacity int, maxMemory uint64) (*CacheSharded, error) {
	numShards := runtime.NumCPU()
	shards := make([]*Shard, numShards)
	consistencyDelay := 10 // play with this value to take different performance metrics
	channels := make([]chan *itemOperation, numShards)
	deleteCh := make(chan string, consistencyDelay)

	cache := &CacheSharded{shards: shards, channels: channels, deleteCh: deleteCh}
	for i := 0; i < numShards; i++ {
		shards[i] = &Shard{
			items: make(map[string]Item, capacity/numShards), // preallocate map with capacity
		}
		channels[i] = make(chan *itemOperation, consistencyDelay) // non-blocking read and write, eventaul consistency (try with 1-10-100)
		go func(ch chan *itemOperation, shard *Shard) {
			for op := range ch {
				shard.Lock()
				shard.items[op.key] = *op.item
				op.item = nil
				shard.Unlock()
			}
		}(channels[i], shards[i])
	}
	// delete goroutine for all shards, lazy delete
	go func() {
		for key := range deleteCh {
			_, shard := cache.getShard(key)
			shard.Lock()
			_, exists := shard.items[key]
			if exists {
				delete(shard.items, key)
			}
			shard.Unlock()
		}
	}()

	return cache, nil
}

func (c *CacheSharded) getShard(key string) (int, *Shard) {
	hash := fnv32(key)
	index := int(hash % uint32(len(c.shards)))
	return index, c.shards[index]
}

func (c *CacheSharded) Set(key string, value interface{}, ttl int) error {
	exp := int64(0)
	if ttl > 0 {
		exp = time.Now().Add(time.Duration(ttl) * time.Second).UnixNano()
	}
	index, _ := c.getShard(key)
	c.channels[index] <- &itemOperation{
		key: key,
		item: &Item{
			value:      value,
			expiration: exp,
		},
	}

	return nil
}

func (c *CacheSharded) Get(key string) (interface{}, bool) {
	_, shard := c.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	item, ok := shard.items[key]
	if !ok {
		return nil, false
	}

	if item.expiration != 0 && item.expiration < time.Now().UnixNano() {
		c.Delete(key) // push into a therad, non waiting
		return nil, false
	}

	return item.value, true
}

func (c *CacheSharded) Delete(key string) {
	c.deleteCh <- key
}

func (c *CacheSharded) Stats() (uint64, int) {
	var totalMemory uint64
	var totalKeys int
	for _, shard := range c.shards {
		shard.RLock()
		totalKeys += len(shard.items)
		shard.RUnlock()
	}
	return totalMemory, totalKeys
}
