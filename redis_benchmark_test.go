package go_kv_db

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func BenchmarkRedisWrite(b *testing.B) {
	benchmarks := []struct {
		name  string
		count int
	}{
		{"1M", 1000000},
		{"5M", 5000000},
		{"10M", 10000000},
		{"20M", 20000000},
	}

	file, err := os.Create("benchmark_redis_write_results.csv")
	if err != nil {
		b.Fatalf("could not create results file: %v", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Benchmark,Count,Time(ms)\n")

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache, err := NewCache(bm.count, 4*1024*1024*1024) // 4GB max memory
			if err != nil {
				b.Fatalf("Error creating cache: %v", err)
			}

			var wg sync.WaitGroup
			numGoroutines := runtime.NumCPU()
			itemsPerGoroutine := bm.count / numGoroutines

			// Benchmark insertion
			start := time.Now()
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						key := fmt.Sprintf("%d_%d", g, i)
						value := "V"
						err := cache.Set(key, value, 0)
						if err != nil {
							b.Fatalf("Error setting item: %v", err)
						}
					}
				}(g)
			}
			wg.Wait()
			duration := time.Since(start).Milliseconds()
			fmt.Fprintf(file, "%s,%d,%d\n", bm.name, bm.count, duration)

			// Clear the cache
			cache = nil
			runtime.GC()

			time.Sleep(time.Second * 5)
		})
	}
}

func BenchmarkRedisReadWrite(b *testing.B) {
	benchmarks := []struct {
		name  string
		count int
	}{
		{"1M", 1000000},
		{"5M", 5000000},
		{"10M", 10000000},
		{"20M", 20000000},
	}

	file, err := os.Create("benchmark_redis_read_write_results.csv")
	if err != nil {
		b.Fatalf("could not create results file: %v", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Benchmark,Count,Time(ms)\n")

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache, err := NewCache(bm.count, 4*1024*1024*1024) // 4GB max memory
			if err != nil {
				b.Fatalf("Error creating cache: %v", err)
			}

			var wg sync.WaitGroup
			numGoroutines := runtime.NumCPU()
			itemsPerGoroutine := bm.count / numGoroutines

			// Benchmark insertion
			start := time.Now()
			for g := 0; g < numGoroutines; g++ {
				worker := g
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						key := fmt.Sprintf("%d_%d", worker, i)
						value := "V"
						err := cache.Set(key, value, 0)
						if err != nil {
							b.Fatalf("Error setting item: %v", err)
						}
					}
				}(worker)
			}
			wg.Wait()
			duration := time.Since(start).Milliseconds()
			fmt.Fprintf(file, "%s,%d,%d\n", bm.name, bm.count, duration)

			// Benchmark read
			start = time.Now()
			for g := 0; g < numGoroutines; g++ {
				worker := g
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						key := fmt.Sprintf("key%d_%d", worker, i)
						_, _ = cache.Get(key)
					}
				}(worker)
			}
			wg.Wait()
			duration = time.Since(start).Milliseconds()
			fmt.Fprintf(file, "%s_Read,%d,%d\n", bm.name, bm.count, duration)

			// Clear the cache
			cache = nil
			runtime.GC()

			time.Sleep(time.Second * 5)
		})
	}
}

func humanReadableBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
