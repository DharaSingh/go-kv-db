package go_kv_db

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func BenchmarkDragonflyWrite(b *testing.B) {
	benchmarks := []struct {
		name  string
		count int
	}{
		{"1M", 1000000},
		{"5M", 5000000},
		{"10M", 10000000},
		{"20M", 20000000},
	}

	file, err := os.Create("benchmark_dragonfly_write_results.csv")
	if err != nil {
		b.Fatalf("could not create results file: %v", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Benchmark,Count,Time(ms)\n")

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache, err := NewDragonflyDB(bm.count, 4*1024*1024*1024) // 4GB max memory
			if err != nil {
				b.Fatalf("Error creating sharded cache: %v", err)
			}

			var wg sync.WaitGroup
			numGoroutines := runtime.NumCPU()
			itemsPerGoroutine := bm.count / numGoroutines

			// Benchmark insertion
			start := time.Now()
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				worker := g
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

			// Clear the cache
			cache = nil
			runtime.GC()

			time.Sleep(time.Second * 5)
		})
	}
}

func BenchmarkDragonflyReadWrite(b *testing.B) {
	benchmarks := []struct {
		name  string
		count int
	}{
		{"1M", 1000000},
		{"5M", 5000000},
		{"10M", 10000000},
		{"20M", 20000000},
	}

	file, err := os.Create("benchmark_dragonfly_read_write.csv")
	if err != nil {
		b.Fatalf("could not create results file: %v", err)
	}
	defer file.Close()

	fmt.Fprintf(file, "Benchmark,Count,Time(ms)\n")

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			cache, err := NewDragonflyDB(bm.count, 4*1024*1024*1024) // 4GB max memory
			if err != nil {
				b.Fatalf("Error creating sharded cache: %v", err)
			}

			var wg sync.WaitGroup
			numGoroutines := runtime.NumCPU()
			itemsPerGoroutine := bm.count / numGoroutines

			// Benchmark insertion
			start := time.Now()
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				worker := g
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

			//	Benchmark read
			start = time.Now()
			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				worker := g
				go func(worker int) {
					defer wg.Done()
					for i := 0; i < itemsPerGoroutine; i++ {
						key := fmt.Sprintf("%d_%d", worker, i)
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
