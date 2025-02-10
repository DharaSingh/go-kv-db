package go_kv_db

import "encoding/json"

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
	expiration int64 // can be pointer to save space in memory, because default 0 will always be applied
}

type Customer struct {
	Name   string `json:"name"`
	Mobile string `json:"mobile"`
}

// Function to serialize Customer to JSON
func (c *Customer) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// Function to deserialize JSON to Customer
func FromJSON(data string) (*Customer, error) {
	var customer Customer
	err := json.Unmarshal([]byte(data), &customer)
	if err != nil {
		return nil, err
	}
	return &customer, nil
}
