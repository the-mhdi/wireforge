package tcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Dialer struct {
	config         *net.Dialer
	Options        *DialOptions
	ConnectionPool *ConnPool // nil if not using a pool
	Closed         <-chan struct{}
	mu             sync.RWMutex
}

type DialOptions struct {
	Verbose bool

	Timeout   time.Duration // Max time to wait for the connection to establish //default no timeout
	Deadline  time.Time
	LocalAddr net.Addr // Optional: Bind to a specific local IP/Interface

	KeepAlive            bool
	KeepAliveFirstProbe  time.Duration
	KeepAliveInterval    time.Duration
	MaxKeepAliveAttempts int

	FallbackDelay time.Duration

	MultipathTCP bool
	TCPFastOpen  bool // Enables TCP Fast Open Connect (Latency reduction)

	CustomDns      bool
	DnsMode        string //udp, tcp , tls, https
	DnsServers     []string
	DnsDialTimeout time.Duration //defalut 6 sec

	WriteBuffer int // Default = 0 (Auto-Tune)
	ReadBuffer  int // Default = 0 (Auto-Tune)

	NoDelay bool // Default true

}

// NewDialer explicitly creates a reusable dialer object
func (do *DialOptions) NewDialer() *Dialer {
	return &Dialer{
		config:  do.convertToDialConfig(),
		Options: do,
	}
}

// DialWithContext is the core dialing logic with Post-Dial tuning
func (d *Dialer) DialWithContext(ctx context.Context, address string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if d.Options.Verbose {
		log.Printf("[Log] Dialing [ %s ]", address)
	}

	// 1. Dial the connection
	conn, err := d.config.DialContext(ctx, "tcp", address)
	if err != nil {
		if d.Options.Verbose {
			log.Printf("[Log] Failed to Dial [ %s ]: %v", address, err)
		}
		return nil, err
	}

	// 2. Post-Dial Tuning (Apply buffers, NoDelay, etc.)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(d.Options.NoDelay)

		if d.Options.ReadBuffer > 0 {
			tcpConn.SetReadBuffer(d.Options.ReadBuffer)
		}

		if d.Options.WriteBuffer > 0 {
			tcpConn.SetWriteBuffer(d.Options.WriteBuffer)

		}

	}

	if d.Options.Verbose {
		log.Printf("[Log] Successfully Dialed And Connedted To [ %s ] Over TCP", address)
	}

	d.Closed = ctx.Done()

	return conn, nil

}

// Dial is a convenience wrapper for DialWithContext
func (d *Dialer) Dial(address string) (net.Conn, error) {
	return d.DialWithContext(context.Background(), address)
}

// DialWithPool wraps this dialer in a Connection Pool.
// You can pass an optional *PoolOptions to configure pool size.
func (d *Dialer) DialWithPool(address string, poolOpts *PoolOptions) (*ConnPool, error) {
	if poolOpts == nil {
		// Use reasonable defaults if the user doesn't provide pool options
		poolOpts = &PoolOptions{
			MaxSize:     20,
			MaxIdleSize: 20,
			MinSize:     2,
			IdleTimeout: 5 * time.Minute,
		}
	}

	// Attach the Dialer's address and Network info to the Pool Options
	poolOpts.Address = address
	poolOpts.Network = "tcp"

	// Tell the pool to use THIS dialer's logic when it needs a new connection
	poolOpts.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialWithContext(ctx, addr)
	}

	// Create and return the pool
	pool, err := poolOpts.New()
	if err != nil {
		return nil, err
	}

	// Link the pool to this dialer object for reference
	d.mu.Lock()
	d.ConnectionPool = pool
	d.mu.Unlock()

	return pool, nil
}

func (do *DialOptions) DialWithContext(ctx context.Context, address string) (net.Conn, error) {
	return do.NewDialer().DialWithContext(ctx, address)

}

func (do *DialOptions) Dial(address string) (net.Conn, error) {
	return do.DialWithContext(context.Background(), address)
}

func (do *DialOptions) DialWithPool(address string, poolOpts *PoolOptions) (*ConnPool, error) {
	return do.NewDialer().DialWithPool(address, poolOpts)
}

// convertToDialConfig handles the OS level and DNS level configurations
func (do *DialOptions) convertToDialConfig() *net.Dialer {
	var r *net.Resolver = nil

	dd := DefaultDialOptions()
	// Override defaults with any user-specified options
	if do != nil {
		if do.Verbose {
			dd.Verbose = do.Verbose
		}
		if do.Timeout != 0 {
			dd.Timeout = do.Timeout
		}
		if do.LocalAddr != nil {
			dd.LocalAddr = do.LocalAddr
		}
		if do.KeepAlive {
			dd.KeepAlive = true
			if do.KeepAliveFirstProbe != 0 {
				dd.KeepAliveFirstProbe = do.KeepAliveFirstProbe
			}
			if do.KeepAliveInterval != 0 {
				dd.KeepAliveInterval = do.KeepAliveInterval
			}
			if do.MaxKeepAliveAttempts != 0 {
				dd.MaxKeepAliveAttempts = do.MaxKeepAliveAttempts
			}
		}
		if do.FallbackDelay != 0 {
			dd.FallbackDelay = do.FallbackDelay
		}
		if do.MultipathTCP {
			dd.MultipathTCP = true
		}
		if do.TCPFastOpen {
			dd.TCPFastOpen = true
		}
		if do.WriteBuffer != 0 {
			dd.WriteBuffer = do.WriteBuffer
		}
		if do.ReadBuffer != 0 {
			dd.ReadBuffer = do.ReadBuffer
		}
		if !do.NoDelay {
			dd.NoDelay = false
		}
	}

	if do.CustomDns && len(do.DnsServers) > 0 {
		r = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				var conn net.Conn
				var err error

				for _, addr := range do.DnsServers {
					// Ensure port 53 or 853 is appended if missing
					if !strings.Contains(addr, ":") {
						if strings.ToUpper(do.DnsMode) == "TLS" {
							addr += ":853" // Default DoT port
						} else {
							addr += ":53" // Default DNS port
						}
					}

					switch strings.ToUpper(do.DnsMode) {
					case "TCP":
						conn, err = net.DialTimeout("tcp", addr, do.DnsDialTimeout)
					case "TLS":
						// DNS over TLS (DoT)
						conn, err = tls.DialWithDialer(&net.Dialer{Timeout: do.DnsDialTimeout}, "tcp", addr, nil)
					default: // UDP
						conn, err = net.DialTimeout("udp", addr, do.DnsDialTimeout)
					}

					if err == nil {
						return conn, nil // Success! Break out of the loop and return.
					}
				}

				// If we get here, all DNS servers failed
				return nil, fmt.Errorf("all configured DNS servers failed, last error: %v", err)
			},
		}
	}

	dc := &net.Dialer{
		Timeout:       dd.Timeout,
		Deadline:      dd.Deadline,
		LocalAddr:     dd.LocalAddr,
		FallbackDelay: dd.FallbackDelay,

		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   dd.KeepAlive,
			Idle:     dd.KeepAliveFirstProbe,
			Interval: dd.KeepAliveInterval,
			Count:    dd.MaxKeepAliveAttempts,
		},

		Control: func(network, address string, c syscall.RawConn) error {
			var socketErr error
			err := c.Control(func(fd uintptr) {

				if do.TCPFastOpen {
					_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, 30, 1)
				}
			})
			if err != nil {
				return err
			}
			return socketErr
		},

		Resolver: r,
	}

	// Native Go 1.21+ MPTCP Support
	dc.SetMultipathTCP(do.MultipathTCP)

	return dc
}

func DefaultDialOptions() *DialOptions {

	return &DialOptions{
		Verbose:              false,
		Timeout:              0, // by default hangs forever
		KeepAlive:            true,
		KeepAliveFirstProbe:  0,
		KeepAliveInterval:    0,
		MaxKeepAliveAttempts: 0,
		FallbackDelay:        0,
		ReadBuffer:           0,
		WriteBuffer:          0,
		NoDelay:              true,
		CustomDns:            false,
		DnsMode:              "UDP",
		DnsDialTimeout:       6 * time.Second,
		MultipathTCP:         false,
		TCPFastOpen:          false,
	}

}

/////////////////////

func (do *DialOptions) WithVerbose(v bool) *DialOptions {
	do.Verbose = v
	return do
}

func (do *DialOptions) WithTimeout(t time.Duration) *DialOptions {
	do.Timeout = t
	return do
}

func (do *DialOptions) WithLocalAddr(ip string) *DialOptions {
	if addr, err := net.ResolveTCPAddr("tcp", ip); err == nil {
		do.LocalAddr = addr
	}
	return do
}

func (do *DialOptions) WithKeepAlive(v bool) *DialOptions {
	do.KeepAlive = v
	return do
}

func (do *DialOptions) WithFastOpen(v bool) *DialOptions {
	do.TCPFastOpen = v
	return do
}

func (do *DialOptions) WithMPTCP(v bool) *DialOptions {
	do.MultipathTCP = v
	return do
}

func (do *DialOptions) WithCustomDNS(mode string, servers ...string) *DialOptions {
	do.CustomDns = true
	do.DnsMode = mode
	do.DnsServers = servers
	return do
}

////////////////////////

func (d *Dialer) updateConfig() {
	d.config = d.Options.convertToDialConfig()
}

func (d *Dialer) SetVerbose(v bool) *Dialer {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Options.Verbose = v
	return d
}

func (d *Dialer) SetTimeout(t time.Duration) *Dialer {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Options.Timeout = t
	d.updateConfig()
	return d
}

func (d *Dialer) SetMPTCP(v bool) *Dialer {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Options.MultipathTCP = v
	d.updateConfig()
	return d
}
