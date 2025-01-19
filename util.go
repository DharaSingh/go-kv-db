package go_kv_db

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

type Item struct {
	value      interface{}
	expiration int64 // can be pointer to save space in memory, becuase default 0 will always be applied
}
