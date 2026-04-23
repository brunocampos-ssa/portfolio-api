package concurrent_test

import (
	"context"
	"testing"
	"time"

	"github.com/brunocampos-ssa/portfolio-api/internal/concurrent"
)

// =============================================================================
// Benchmarks — primarily pedagogical. Students use these to see how worker
// count affects throughput for CPU-bound vs I/O-bound tasks.
//
// Example session:
//
//	go test ./internal/concurrent -bench=BenchmarkWorkerPool -benchmem -run=^$
//
// Combined with `-cpuprofile=cpu.out` and `go tool pprof cpu.out`, this makes
// a good exercise in spotting contention (mutex, channel send) vs actual work.
// =============================================================================

// BenchmarkWorkerPool_CPU exercises the happy path: cheap CPU-only work.
// Expected result: more workers ≠ much faster. The channel overhead dominates.
func BenchmarkWorkerPool_CPU(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16} {
		b.Run("workers="+itoa(n), func(b *testing.B) {
			benchmarkWorkerPool(b, n, 1_000, func(x int) int { return x * 2 })
		})
	}
}

// BenchmarkWorkerPool_IO simulates I/O by sleeping per task.
// Expected result: throughput scales with worker count until the test
// machine's goroutine scheduler overhead catches up.
func BenchmarkWorkerPool_IO(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16} {
		b.Run("workers="+itoa(n), func(b *testing.B) {
			benchmarkWorkerPool(b, n, 200, func(x int) int {
				time.Sleep(50 * time.Microsecond)
				return x
			})
		})
	}
}

func benchmarkWorkerPool(b *testing.B, numWorkers, items int, work func(int) int) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		input := make(chan int, items)
		for i := range items {
			input <- i
		}
		close(input)

		output := concurrent.RunWorkerPool(context.Background(), numWorkers, input, func(_ context.Context, x int) int {
			return work(x)
		})
		for range output {
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = '0' + byte(n%10)
		n /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
