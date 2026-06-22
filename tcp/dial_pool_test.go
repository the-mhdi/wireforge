package tcp

import (
	"context"
	"net"
	"testing"
)

// --- MOCKS ---

// mockConn is a fake connection that doesn't actually hit the network.
// It allows us to benchmark pure pool performance.
type mockConn struct{ net.Conn }

func (m *mockConn) Close() error { return nil }

func mockDialFunc(ctx context.Context, network string, address string) (net.Conn, error) {
	return &mockConn{}, nil
}

// --- BENCHMARKS ---

// BenchmarkPoolSequential tests how fast a single goroutine can Get and Put.
// This tests the "Fast Path" of your channel logic.
func BenchmarkPoolSequential(b *testing.B) {
	opts := &PoolOptions{
		MaxSize:     100,
		MaxIdleSize: 100,
		MinSize:     100, // Warm up 100 connections instantly
		DialFunc:    mockDialFunc,
	}

	pool, err := opts.New()
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer() // Reset timer so setup isn't included in the benchmark score
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
		pool.Put(conn, nil)
	}
}

// BenchmarkPoolParallel tests how the pool performs when thousands of
// concurrent goroutines are fighting for connections.
// This tests your CAS (CompareAndSwap) and channel contention.
func BenchmarkPoolParallel(b *testing.B) {
	opts := &PoolOptions{
		MaxSize:     1000,
		MaxIdleSize: 1000,
		MinSize:     1000, // Pre-fill to avoid dial overhead in the bench
		DialFunc:    mockDialFunc,
	}

	pool, err := opts.New()
	if err != nil {
		b.Fatalf("failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get(ctx)
			if err == nil {
				pool.Put(conn, nil)
			}
		}
	})
}
