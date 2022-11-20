package limiter_test

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/JaderDias/limiter"

	. "github.com/onsi/gomega"
)

func TestExample(t *testing.T) {
	RegisterTestingT(t)

	t.Run("TestExample", func(*testing.T) {
		x := int32(1000)
		limiter.BoundedConcurrency(10, 1000, func(i int) {
			// do some work:
			atomic.AddInt32(&x, -1)
		})
		Expect(x).To(BeEquivalentTo(0))
	})
}

func TestWithDoneProcessor(t *testing.T) {
	RegisterTestingT(t)

	tests := []struct {
		name             string
		concurrencyLimit int
		numberOfTasks    int
	}{
		{
			name:             "MoreTasksThanConcurrencyLimit",
			concurrencyLimit: 10,
			numberOfTasks:    100,
		},
		{
			name:             "SameTasksAndConcurrencyLimit",
			concurrencyLimit: 100,
			numberOfTasks:    100,
		},
		{
			name:             "MoreLimitThanTasks",
			concurrencyLimit: 100,
			numberOfTasks:    10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(*testing.T) {
			nrDoneTasks := int32(0)
			nrDoneTasksWhenFirstDoneInvoked := int32(0)
			results := make([]int32, tt.numberOfTasks)
			limiter.BoundedConcurrencyWithDoneProcessor(
				tt.concurrencyLimit,
				tt.numberOfTasks,
				func(i int) int32 {
					// do some work:
					return atomic.AddInt32(&nrDoneTasks, 1)
				},
				func(result int32) {
					atomic.CompareAndSwapInt32(&nrDoneTasksWhenFirstDoneInvoked, int32(0), nrDoneTasks)
					results[int(result-1)] = result
				},
			)
			Expect(nrDoneTasks).To(BeEquivalentTo(tt.numberOfTasks))
			Expect(nrDoneTasksWhenFirstDoneInvoked).To(BeNumerically(">", 0))
			Expect(int(nrDoneTasksWhenFirstDoneInvoked)).To(BeNumerically("<=", tt.numberOfTasks))
			Expect(int(nrDoneTasksWhenFirstDoneInvoked)).To(BeNumerically("<=", tt.concurrencyLimit))
			for i, v := range results {
				Expect(i).To(BeNumerically(">=", 0))
				Expect(i).To(BeNumerically("<", tt.numberOfTasks))
				Expect(v).To(BeNumerically(">", 0))
				Expect(v).To(BeNumerically("<=", int32(tt.numberOfTasks)))
			}
		})
	}
}

func TestLimit(t *testing.T) {
	RegisterTestingT(t)

	t.Run("TestLimit", func(*testing.T) {
		concurrencyLimit := 10
		numberOfTasks := 1000
		m := map[int]bool{}
		lock := &sync.Mutex{}
		max := int32(0)
		concurrent := int32(0)
		limiter.BoundedConcurrency(concurrencyLimit, numberOfTasks, func(i int) {
			atomic.AddInt32(&concurrent, 1)
			lock.Lock()
			m[i] = true
			lock.Unlock()
			if concurrent > max {
				max = concurrent
			}
			atomic.AddInt32(&concurrent, -1)
		})

		Expect(len(m)).To(BeEquivalentTo(numberOfTasks))
		Expect(max).To(BeEquivalentTo(int32(concurrencyLimit)))
	})
}

func TestConcurrentIO(t *testing.T) {
	RegisterTestingT(t)

	t.Run("TestConcurrentIO", func(*testing.T) {
		urls := []string{
			"http://www.google.com",
			"http://www.apple.com",
		}
		results := make([]int, 2)
		limiter.BoundedConcurrency(10, 2, func(i int) {
			resp, err := http.Get(urls[i])
			Expect(err).To(BeNil())
			defer resp.Body.Close()
			results[i] = resp.StatusCode
		})

		Expect(results[0]).To(BeEquivalentTo(200))
		Expect(results[1]).To(BeEquivalentTo(200))
	})
}

func TestConcurrently(t *testing.T) {
	RegisterTestingT(t)

	t.Run("TestConcurrently", func(*testing.T) {
		errors := []error{
			errors.New("error a"),
			errors.New("error b"),
		}
		var firstError atomic.Value
		completed := int32(0)
		limiter.BoundedConcurrency(4, 2, func(i int) {
			atomic.AddInt32(&completed, 1)
			// Do some really slow IO ...
			// keep the error:
			firstError.CompareAndSwap(nil, errors[i])
		})

		Expect(completed).To(BeEquivalentTo(2))
		firstErrorValue := firstError.Load().(error)
		Expect(firstErrorValue).ToNot(BeNil())
		Expect(firstErrorValue == errors[0] || firstErrorValue == errors[1]).To(BeTrue())
	})
}

func TestEmpty(t *testing.T) {
	RegisterTestingT(t)

	t.Run("TestEmpty", func(*testing.T) {
		limiter.BoundedConcurrency(4, 0, func(i int) {
		})
	})
}

func Benchmark_10tasks_numWorkers1(b *testing.B) {
	benchmark(b, 10, 1)
}

func Benchmark_100tasks_numWorkers10(b *testing.B) {
	benchmark(b, 100, 10)
}

func Benchmark_10Ktasks_numWorkers100(b *testing.B) {
	benchmark(b, 10000, 100)
}

func Benchmark_10Ktasks_numWorkers1000(b *testing.B) {
	benchmark(b, 10000, 1000)
}

func benchmark(b *testing.B, numberOfTasks, numberOfWorkers int) {
	for i := 0; i < b.N; i++ {
		x := int32(numberOfTasks)
		limiter.BoundedConcurrency(numberOfWorkers, numberOfTasks, func(i int) {
			// do some work:
			atomic.AddInt32(&x, -1)
		})
	}
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

func TestMemory(t *testing.T) {
	numberOfTasks := 100000
	numberOfWorkers := 1000
	for i := 0; i < 10; i++ {
		x := int32(numberOfTasks)
		limiter.BoundedConcurrency(numberOfWorkers, numberOfTasks, func(i int) {
			if i == numberOfTasks-1 {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				fmt.Printf("Alloc = %v KiB", bToKb(m.Alloc))
				fmt.Printf("\tTotalAlloc = %v KiB", bToKb(m.TotalAlloc))
				fmt.Printf("\tSys = %v KiB", bToKb(m.Sys))
				fmt.Printf("\tNumGC = %v\n", m.NumGC)
			}

			atomic.AddInt32(&x, -1)
		})
	}
}
