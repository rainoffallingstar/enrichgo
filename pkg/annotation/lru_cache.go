package annotation

import "container/list"

type lruCache struct {
	max     int
	ll      *list.List
	items   map[string]*list.Element
	evicted uint64
}

type lruEntry struct {
	key string
	val []string
}

func newLRUCache(maxEntries int) *lruCache {
	return &lruCache{
		max:   maxEntries,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

func (c *lruCache) Len() int {
	if c == nil {
		return 0
	}
	return len(c.items)
}

func (c *lruCache) Get(key string) ([]string, bool) {
	if c == nil {
		return nil, false
	}
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		ent := el.Value.(*lruEntry)
		return ent.val, true
	}
	return nil, false
}

func (c *lruCache) Set(key string, val []string) {
	if c == nil {
		return
	}
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*lruEntry).val = val
		return
	}
	el := c.ll.PushFront(&lruEntry{key: key, val: val})
	c.items[key] = el
	c.evictIfNeeded()
}

func (c *lruCache) evictIfNeeded() {
	if c == nil || c.max <= 0 {
		return
	}
	for len(c.items) > c.max {
		el := c.ll.Back()
		if el == nil {
			return
		}
		c.ll.Remove(el)
		ent := el.Value.(*lruEntry)
		delete(c.items, ent.key)
		c.evicted++
	}
}

func (c *lruCache) Evicted() uint64 {
	if c == nil {
		return 0
	}
	return c.evicted
}
