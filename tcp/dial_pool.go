package tcp

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed = errors.New("connection pool is closed")
	ErrPoolFull   = errors.New("connection pool reached MaxSize")
	ErrConnDead   = errors.New("connection is dead")
)

// lock-free connection pool with background sorting and connection manager
type ConnPool struct {
	Options *PoolOptions

	// idleQueue is our highly-concurrent, lock-free-style queue
	idleQueue chan *conn

	// activeConns tracks total connections (idle + in-use) safely
	activeConns atomic.Int32
	isClosed    atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

type PoolOptions struct {
	DialFunc func(ctx context.Context, network string, address string) (net.Conn, error)

	MaxSize     int //maximum idle+inuse connections inside the pool  //default is 20
	MaxIdleSize int //maximum idle connections inside the pool =< maxSize, default is == to maxsize

	MinSize int //default 0

	HealthCheck    bool                      //default true
	GetHealthCheck func(conn net.Conn) error //healthChecks a single connection right before it gets returned by Get()

	IdleTimeout time.Duration //default 0

	// Metadata
	Address string
	Network string
}

type conn struct {
	conn      net.Conn
	idleSince time.Time
	//state      atomic.Int32 // 1 = idle , 2 = inuse

	//Latency    time.Duration
	//ReadCalls  int
	//WriteCalls int
}

// NewPool initializes and warms up the connection pool
func (opts *PoolOptions) New() (*ConnPool, error) {
	if opts.MaxIdleSize < 1 {
		opts.MaxIdleSize = 1 // Prevent unbuffered channel deadlock
	}

	if opts.MaxIdleSize < opts.MinSize {
		opts.MaxIdleSize = opts.MinSize
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &ConnPool{
		Options:   opts,
		idleQueue: make(chan *conn, opts.MaxIdleSize),
		ctx:       ctx,
		cancel:    cancel,
	}

	// 1. Warm up the pool (MinSize)
	for i := 0; i < opts.MinSize; i++ {
		c, err := p.Options.DialFunc(ctx, p.Options.Network, p.Options.Address)
		if err != nil {
			cancel() // Abort if we can't meet the minimum requirements
			return nil, err
		}
		p.activeConns.Add(1)
		p.idleQueue <- &conn{conn: c, idleSince: time.Now()}
	}

	// 2. Start the background Reaper to clean up expired idle connections
	if opts.IdleTimeout > 0 {
		go p.reaper()
	}

	return p, nil
}

// Get retrieves a connection from the pool, or dials a new one if needed.
// ctx allows the user to set a timeout (e.g., "wait up to 5s for a connection")
func (p *ConnPool) Get(ctx context.Context) (net.Conn, error) {
	if p.isClosed.Load() {
		return nil, ErrPoolClosed
	}

	for {
		select {
		// 1. Fast Path: Grab an existing idle connection
		case ic := <-p.idleQueue:
			// Check if it timed out while sitting in the queue
			if p.Options.IdleTimeout > 0 && time.Since(ic.idleSince) > p.Options.IdleTimeout {
				ic.conn.Close()
				p.activeConns.Add(-1)
				continue // Loop again to try the next one
			}

			// Run Health Check if configured
			if p.Options.HealthCheck && p.Options.GetHealthCheck != nil {
				if err := p.Options.GetHealthCheck(ic.conn); err != nil {
					ic.conn.Close()
					p.activeConns.Add(-1)
					continue
				}
			}

			return ic.conn, nil

		default:
			// 2. Slow Path: No idle connections available.
			// Check if we are allowed to dial a new one.
			currentActive := p.activeConns.Load()
			if int(currentActive) < p.Options.MaxSize {
				// We have room to grow! Safely increment the counter.
				if p.activeConns.CompareAndSwap(currentActive, currentActive+1) {
					conn, err := p.Options.DialFunc(ctx, p.Options.Network, p.Options.Address)
					if err != nil {
						p.activeConns.Add(-1) // Dial failed, revert counter
						return nil, err
					}
					return conn, nil
				}
				continue // CAS failed (another goroutine grabbed the slot), retry
			}

			// 3. Pool is completely FULL. We must wait for someone to Put() one back.
			select {
			case ic := <-p.idleQueue:
				// We waited and got one!
				return ic.conn, nil
			case <-ctx.Done():
				// User's context timed out before a connection became available
				return nil, ctx.Err()
			case <-p.ctx.Done():
				return nil, ErrPoolClosed
			}
		}
	}
}

// Put returns a connection to the pool.
// If err != nil (connection broke), it is discarded.
func (p *ConnPool) Put(c net.Conn, err error) {
	if c == nil {
		return
	}

	if p.isClosed.Load() || err != nil {
		// Pool is closed, or connection is broken. Throw it away.
		c.Close()
		p.activeConns.Add(-1)
		return
	}

	// Try to return it to the idle queue
	select {
	case p.idleQueue <- &conn{conn: c, idleSince: time.Now()}:
		// Successfully returned to pool
	default:
		// The idle queue is full (MaxIdleSize reached). Discard the connection.
		c.Close()
		p.activeConns.Add(-1)
	}
}

// reaper runs in the background and cleans up connections that sit idle for too long
func (p *ConnPool) reaper() {
	ticker := time.NewTicker(p.Options.IdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return // Pool closed
		case <-ticker.C:
			p.evictOldConnections()
		}
	}
}
func (p *ConnPool) evictOldConnections() {
	// Check how many connections are currently idle
	idleCount := len(p.idleQueue)

	for range idleCount {
		select {
		case ic := <-p.idleQueue:
			if time.Since(ic.idleSince) > p.Options.IdleTimeout {
				// It's too old. Kill it.
				ic.conn.Close()
				p.activeConns.Add(-1)
			} else {
				// It's still good! Put it back in the queue.
				p.idleQueue <- ic
			}
		default:
			// Queue is empty, nothing to do
			return
		}
	}
}
func (p *ConnPool) Close() {
	if p.isClosed.CompareAndSwap(false, true) {
		p.cancel() // Stop the reaper and wake up waiting Gets

		// Drain and close all idle connections
		close(p.idleQueue)
		for ic := range p.idleQueue {
			ic.conn.Close()
		}
	}
}

func (p *ConnPool) Stats() (active int, idle int) {
	return int(p.activeConns.Load()), len(p.idleQueue)
}
