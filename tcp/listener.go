package tcp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/atomic"

	"golang.org/x/sys/unix"
)

type acceptErrClass int

const (
	acceptErrFatal    acceptErrClass = iota // listener is broken; exit the loop
	acceptErrBackoff                        // transient (EMFILE etc.); sleep and retry
	acceptErrRetryNow                       // client-side noise; retry immediately
)

type handlerFunc func(ctx context.Context, conn net.Conn)

type Listener struct {
	Address     string
	isListening bool
	listener    net.Listener
	config      *net.ListenConfig
	Options     *ListenOptions

	connectionPool sync.Map

	mu     sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	ipConnCountMu sync.RWMutex
	ipConnCount   map[string]atomic.Uint32

	connCountMu sync.RWMutex
	connCount   atomic.Uint32

	ErrCh     chan error
	errMu     sync.Mutex // guards ErrCh sends and close
	errClosed bool
}

type ListenOptions struct {
	Verbose bool

	//incomming connections configuration
	Inbounds InboundConnOptions

	ReuseAddr bool // Bypass TIME_WAIT on restart
	ReusePort bool // Leave false by default unless specifically needed, Allow multiple listeners on the same port for multi-core scaling

	//TCPFastOpen  bool
	TCPFastOpenQueue int  //default 256
	MultipathTCP     bool //only works if OS supports

	//the max time the Stop() method can try closing and draining the remaining connections for a graceful shutdown
	ShutdownTimeout time.Duration //default 15s

	MaxConnections      uint32
	MaxConnectionsPerIP uint32 // 0 == no limit

	AcceptFailureRetryDelay time.Duration // default is 5ms, exponential backoff up to 1s

}

// keepAlive options on ListenOptions gets applied to all Incoming conncetions
type InboundConnOptions struct {
	NoDelay     bool //default = true (if os supports)
	WriteBuffer int  // default is 0 = Let OS Auto-Tune dynamically
	ReadBuffer  int  // default is 0 = Let OS Auto-Tune dynamically

	Deadline time.Duration //== absolute deadline , default is 0 == no timeout //the absolute time that an inbound conncetion lives

	DrainConnectionOnClose int // default -1 // == Linger//by default (-1) the operating system finishes sending the data in the background

	KeepAlive bool

	// the time before the first keep-alive probe is sent.
	KeepAliveFirstProbe time.Duration // If zero, a default value of 15 seconds is used.

	// time between keep-alive probes
	KeepAliveInterval time.Duration //If zero,a default value of 15 seconds is used.

	MaxKeepAliveAttempts int // If zero, a default value of 9 is used.
}

func NewListener(opts *ListenOptions) *Listener {

	if opts == nil {
		opts = DefaultListenOptions()
	}

	return opts.newListenerWithContext(context.Background())
}

func NewListenerWithContext(ctx context.Context, opts *ListenOptions) *Listener {
	if opts == nil {
		opts = DefaultListenOptions()
	}

	return opts.newListenerWithContext(ctx)
}

func (lo *ListenOptions) newListenerWithContext(ctx context.Context) *Listener {

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithCancel(ctx)

	ln := &Listener{
		Options:        lo,
		connectionPool: sync.Map{},
		config:         lo.convertToListenConfig(),
		ErrCh:          make(chan error, 3),
		ctx:            ctx,
		cancel:         cancel,
		ipConnCount:    make(map[string]atomic.Uint32),
		//connCount:      atomic.Uint32{},
	}

	return ln
}

func (lo *ListenOptions) ListenWithContext(ctx context.Context, addr string, handler func(context.Context, net.Conn)) {

	ln := lo.newListenerWithContext(ctx)
	err := ln.Initialize(addr, handler)
	if err != nil {
		log.Printf("ERROR STARTING TCP LISTENER: %s", err)
		return
	}

	ln.RunUntilSignal()

}

// listen method here is a  blocking call that automatocally handels the life cycle of tcp listener
// for more control over the life cycle of your listener see NewListener() and listener.Initialize() and Run() method
func (lo *ListenOptions) Listen(addr string, handler func(context.Context, net.Conn)) {
	lo.ListenWithContext(context.Background(), addr, handler)

}

func (ln *Listener) Listen(addr string, handler func(context.Context, net.Conn)) {
	if ln.Options == nil {
		ln.Options = DefaultListenOptions()
	}

	ln.Options.ListenWithContext(context.Background(), addr, handler)

}

// initializes the main listener and starts the acceptLoop go routine, listerner.Run() method "could" be called after this.
func (ln *Listener) Initialize(addr string, handler func(context.Context, net.Conn)) error {

	ln.mu.Lock()
	defer ln.mu.Unlock()

	if ln.isListening {
		panic("[PANIC] :::: TCP Listener Already Listening")
	}

	var err error

	ln.listener, err = ln.config.Listen(ln.ctx, "tcp", addr)

	if err != nil {
		return err
	}
	ln.isListening = true

	ln.Address = ln.listener.Addr().String()

	ln.wg.Add(1)

	go ln.acceptLoop(handler)

	return nil
}

func (ln *Listener) acceptLoop(handler func(context.Context, net.Conn)) {
	defer ln.wg.Done()

	var tempDelay time.Duration // How long to sleep on accept failure

	for {

		inbound, err := ln.listener.Accept()

		if err != nil {

			select {
			case <-ln.ctx.Done():
				// Server is shutting down
				log.Printf("[Log] :::: Listener shutting down...")
				return

			//case ln.ErrCh <- fmt.Errorf("Accept Failure - retry in %v: %v", tempDelay, err):

			default:
				// If channel is full, we log to stderr so we don't lose the error
				//	log.Printf("[wireforge] Error channel full, dropped error: %v", err)
			}

			ln.sendError(fmt.Errorf("Accept Failure - retry in %v: %v", tempDelay, err))

			// Handle temporary errors (like running out of File Descriptors)
			switch classifyAcceptError(err) {

			case acceptErrBackoff:
				if tempDelay == 0 {
					if ln.Options.AcceptFailureRetryDelay != 0 {
						tempDelay = ln.Options.AcceptFailureRetryDelay
					} else {
						tempDelay = 5 * time.Millisecond
					}
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				// Log unconditionally, not just Verbose: this class means
				// "the server cannot take new connections right now".
				log.Printf("[wireforge] :::: Accept error on %s, retrying in %v: %v",
					ln.Address, tempDelay, err)

				// Nap, but abort instantly if shutdown starts, so Stop()
				// never waits out a backoff timer.
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-ln.ctx.Done():
					timer.Stop()
					return
				}
				continue

			case acceptErrRetryNow:
				continue

			default:
				// Genuinely fatal (EBADF, EINVAL, ENOTSOCK, ...): the listener
				// is unusable. Report it, wake Run(), and get out.
				fatalErr := fmt.Errorf("fatal accept error on %s: %w", ln.Address, err)
				log.Print(fatalErr)
				ln.sendError(fatalErr)
				ln.cancel() // without this, Run() blocks until SIGINT with a dead listener
				return
			}
		}

		tempDelay = 0 // Reset delay on success

		// Post-Accept Tuning
		if tcpConn, ok := inbound.(*net.TCPConn); ok {
			ln.incrementConnCount(tcpConn.RemoteAddr())

			if ln.Options.MaxConnections != 0 && ln.getConnCount() >= ln.Options.MaxConnections {
				log.Printf("[wireforge] :::: reached max number of connections")
				inbound.Close()
				continue
			}

			if ln.Options.MaxConnectionsPerIP != 0 && ln.getIpConnCount(tcpConn.RemoteAddr()) > ln.Options.MaxConnectionsPerIP {
				log.Printf("[wireforge] :::: reached max number of connections for this ip %s", tcpConn.RemoteAddr().String())
				inbound.Close()
				continue
			}

			tcpConn.SetNoDelay(ln.Options.Inbounds.NoDelay)

			// If they are 0, we let the Linux Kernel Auto-Tune them dynamically.
			if ln.Options.Inbounds.ReadBuffer > 0 {
				tcpConn.SetReadBuffer(ln.Options.Inbounds.ReadBuffer)
			}

			if ln.Options.Inbounds.WriteBuffer > 0 {
				tcpConn.SetWriteBuffer(ln.Options.Inbounds.WriteBuffer)
			}

			if ln.Options.Inbounds.DrainConnectionOnClose >= 0 {
				tcpConn.SetLinger(ln.Options.Inbounds.DrainConnectionOnClose)
			}

			if ln.Options.Inbounds.Deadline > 0 {
				tcpConn.SetDeadline(time.Now().Add(ln.Options.Inbounds.Deadline))
			}

		}

		ln.wg.Add(1)
		go func(lsn *Listener, c net.Conn, handlerFunc func(context.Context, net.Conn)) {
			defer lsn.wg.Done()
			defer c.Close()
			defer lsn.decrementConnCount(c.RemoteAddr())

			defer func() {
				if r := recover(); r != nil {
					lsn.sendError(fmt.Errorf("handler panic for %s: %v", c.RemoteAddr(), r))
					if lsn.Options.Verbose {
						log.Printf("[wireforge] PANIC RECOVERED: %v\n%s", r, debug.Stack())
					}
				}
			}()

			if lsn.Options.Verbose {
				log.Printf("[wireforge] :::: New CONNECTION [ %s ] ACCEPTED by the Listener [ %s ] ", c.RemoteAddr().String(), lsn.Address)
			}

			lsn.connectionPool.Store(c, struct{}{})
			defer lsn.connectionPool.Delete(c)

			//onDisconnect being here ensures that it only fires if onConnect has been successful,
			// if this go routine is returned at onConnect (if the connection is rejected by onConnect), the onDisconnect logic won't be executed

			if lsn.Options.Verbose {
				log.Printf("[wireforge] :::: CONNECTION ESTABLISHED --- [ %s ] CONNECTED to Listener [ %s ] ", c.RemoteAddr().String(), lsn.Address)
			}

			handlerFunc(lsn.ctx, c) //Execute the user's logic - usually a blocking call

			if lsn.Options.Verbose {
				log.Printf("[wireforge] :::: [ %s ] DISCONNECTED from Listener [ %s ] ", c.RemoteAddr().String(), lsn.Address)
			}

		}(ln, inbound, handler)

	}
}

func (ln *Listener) incrementConnCount(addr net.Addr) {

	ln.connCountMu.Lock()
	ln.connCount.Inc()
	ln.connCountMu.Unlock()

	ln.ipConnCountMu.Lock()
	defer ln.ipConnCountMu.Unlock()
	// Extract IP address from the full address
	ip := strings.Split(addr.String(), ":")[0]
	if count, ok := ln.ipConnCount[ip]; ok {
		count.Inc()
	} else {
		ln.ipConnCount[ip] = atomic.Uint32{}
		count, _ := ln.ipConnCount[ip]

		count.Inc()
	}

}

func (ln *Listener) decrementConnCount(addr net.Addr) {
	ln.connCountMu.Lock()
	ln.connCount.Dec()
	ln.connCountMu.Unlock()

	ln.ipConnCountMu.Lock()
	defer ln.ipConnCountMu.Unlock()
	// Extract IP address from the full address
	ip := strings.Split(addr.String(), ":")[0]

	if count, ok := ln.ipConnCount[ip]; ok {
		c := count.Dec()
		if c == 0 {
			delete(ln.ipConnCount, ip)
		}
	}

}

func (ln *Listener) getIpConnCount(addr net.Addr) uint32 {
	ln.ipConnCountMu.RLock()
	defer ln.ipConnCountMu.RUnlock()
	ip := strings.Split(addr.String(), ":")[0]
	if count, ok := ln.ipConnCount[ip]; ok {
		return count.Load()
	}
	return 0
}

func (ln *Listener) getConnCount() uint32 {
	ln.connCountMu.RLock()
	defer ln.connCountMu.RUnlock()
	return ln.connCount.Load()
}

// just a blocking call waiting for the main listener to be shutdown
// Run blocks until the listener's context is canceled — by Stop, a parent
// context, or a fatal accept error — then drains connections gracefully.
// It does not install signal handlers; signal handling belongs to the
// application. See RunUntilSignal for the opt-in convenience wrapper.
func (ln *Listener) Run() {
	go ln.drainErrors()

	log.Printf("[wireforge] TCP LISTENER STARTED SUCCESSFULLY, Listening on %s", ln.Address)

	<-ln.ctx.Done()
	log.Printf("Shutting down listener [%s] gracefully...", ln.Address)

	ln.shutdown()

	log.Printf("TCP Listener [%s] successfully stopped. All connections closed.", ln.Address)
}

// RunUntilSignal is Run plus opt-in SIGINT/SIGTERM handling: it blocks until
// one of those signals arrives (or the context is canceled), then shuts down
// gracefully. A second signal during the drain force-closes all connections
// instead of waiting out the ShutdownTimeout.
func (ln *Listener) RunUntilSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig) // restores the default disposition when we return

	go ln.drainErrors()

	log.Printf("[wireforge] TCP LISTENER STARTED SUCCESSFULLY, Listening on %s", ln.Address)

	select {
	case s := <-sig:
		log.Printf("%v caught: tcp listener [%s] is being stopped (send again to force)", s, ln.Address)
	case <-ln.ctx.Done():
		log.Printf("Shutting down listener [%s] gracefully...", ln.Address)
	}

	drained := make(chan struct{})
	go func() {
		ln.shutdown()
		close(drained)
	}()

	select {
	case <-drained:
		log.Printf("TCP Listener [%s] successfully stopped. All connections closed.", ln.Address)
	case s := <-sig:
		log.Printf("%v caught during shutdown: force-closing all connections", s)
		ln.forceCloseAllConns()
		<-drained // Stop unblocks once the force-closed handlers unwind; still bounded by the timeout
		log.Printf("TCP Listener [%s] force-stopped.", ln.Address)
	}
}

func (ln *Listener) drainErrors() {
	for err := range ln.ErrCh {
		if ln.Options.Verbose {
			log.Println(err)
		}
	}
}

func (ln *Listener) shutdown() {
	if err := ln.Stop(ln.Options.ShutdownTimeout); err != nil {
		log.Printf("Error during listener [%s] shutdown: %v", ln.Address, err)
	}
}

func (ln *Listener) forceCloseAllConns() {
	ln.connectionPool.Range(func(key, _ any) bool {
		if c, ok := key.(net.Conn); ok {
			c.Close()
		}
		return true
	})
}

func (ln *Listener) Stop(timeout time.Duration) error {
	ln.mu.Lock()

	if !ln.isListening {
		ln.mu.Unlock()
		return nil // Already stopped, do nothing safely
	}

	ln.isListening = false

	ln.mu.Unlock()

	// 1. Tell all goroutines to stop
	ln.cancel()

	defer ln.closeErrCh() // Close error channel to signal no more errors will be sent

	// 2. Close the listener to stop accepting NEW connections
	var err error
	if ln.listener != nil {
		err = ln.listener.Close()
		if err != nil {
			log.Printf("[wireforge] :::: TCP listener closed with error: %v.", err)
		}
		log.Printf("[wireforge] :::: TCP listener closed With No Error")

	}

	// 3. Create a channel to tell us when all connections are naturally closed
	cg := make(chan struct{})
	go func() {
		ln.wg.Wait() // Wait for all handleConnection() to finish
		close(cg)    // Signal that we are 100% done
	}()

	// 4. The "Race": Wait for either the clean shutdown OR the timeout
	select {
	case <-cg:
		log.Printf("[wireforge] :::: All connections To Listener [%s] closed gracefully.", ln.Address)
		return err

	case <-time.After(timeout):
		// The timeout was reached before wg.Wait() finished!
		log.Printf("[wireforge] :::: Listener Shutdown timeout of %v reached! Forcing Exit.", timeout)

		ln.forceCloseAllConns()

		return nil
	}
}

func (ln *Listener) sendError(err error) {
	ln.errMu.Lock()
	defer ln.errMu.Unlock()

	if ln.errClosed {
		return // shutting down; drop the error
	}

	select {
	case ln.ErrCh <- err:
	default:
		if ln.Options.Verbose {
			log.Printf("[wireforge] Error channel full, dropping error: %v", err)
		}
	}
}

// closeErrCh closes ErrCh exactly once and is safe to call while
// other goroutines may still be calling sendError.
func (ln *Listener) closeErrCh() {
	ln.errMu.Lock()
	defer ln.errMu.Unlock()

	if ln.errClosed {
		return
	}
	ln.errClosed = true
	close(ln.ErrCh)
}

func (ln *Listener) Close() error {
	return ln.Stop(0)
}

func (lo *ListenOptions) convertToListenConfig() *net.ListenConfig {

	lc := &net.ListenConfig{
		// Go 1.23+
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   lo.Inbounds.KeepAlive,
			Idle:     lo.Inbounds.KeepAliveFirstProbe,
			Interval: lo.Inbounds.KeepAliveInterval,
			Count:    lo.Inbounds.MaxKeepAliveAttempts,
		},

		// Control allows us to execute low-level OS socket commands before the port is bound
		Control: func(network, address string, c syscall.RawConn) error {
			var socketErr error
			err := c.Control(func(fd uintptr) {
				// 1. SO_REUSEADDR
				if lo.ReuseAddr {
					if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
						socketErr = err
					}
				}

				// 2. SO_REUSEPORT (Note: Not natively supported on Windows)
				if lo.ReusePort {
					if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
						socketErr = err
					}
				}

				// 3. TCP_FASTOPEN (Linux specific TCP option)
				if lo.TCPFastOpenQueue > 0 {
					if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_FASTOPEN, lo.TCPFastOpenQueue); err != nil {
						socketErr = err
					}

				}
			})

			if err != nil {
				return err
			}
			return socketErr
		},
	}

	//Supported in Kernel 5.6 and newer. You usually have to enable it via sysctl: sysctl -w net.mptcp.enabled=1
	lc.SetMultipathTCP(lo.MultipathTCP)

	return lc
}

func DefaultListenOptions() *ListenOptions {

	inboundOps := InboundConnOptions{
		NoDelay:                true,
		WriteBuffer:            0,  // 0 = Let OS Auto-Tune dynamically up to 6MB+
		ReadBuffer:             0,  // 0 = Let OS Auto-Tune dynamically up to 6MB+
		Deadline:               0,  //no Dealine by defalut
		DrainConnectionOnClose: -1, //Linger

		KeepAlive: true,

		KeepAliveFirstProbe:  0, //== def 15
		KeepAliveInterval:    0, //== def 15
		MaxKeepAliveAttempts: 0, //== def 9

	}

	return &ListenOptions{
		Verbose: true,

		ReuseAddr:           true,
		ReusePort:           false, // Leave false by default unless specifically needed
		TCPFastOpenQueue:    256,
		MultipathTCP:        true,
		Inbounds:            inboundOps,
		ShutdownTimeout:     15 * time.Second,
		MaxConnectionsPerIP: 0,
	}
}

//helpers - ai generated

func (lo *ListenOptions) WithVerbose(v bool) *ListenOptions {
	lo.Verbose = v
	return lo
}

func (lo *ListenOptions) WithKeepAlive(v bool) *ListenOptions {
	lo.Inbounds.KeepAlive = v
	return lo
}

func (lo *ListenOptions) WithKeepAliveFirstProbe(d time.Duration) *ListenOptions {
	lo.Inbounds.KeepAliveFirstProbe = d
	return lo
}

func (lo *ListenOptions) WithKeepAliveInterval(d time.Duration) *ListenOptions {
	lo.Inbounds.KeepAliveInterval = d
	return lo
}

func (lo *ListenOptions) WithMaxKeepAliveAttempts(count int) *ListenOptions {
	lo.Inbounds.MaxKeepAliveAttempts = count
	return lo
}

func (lo *ListenOptions) WithReuseAddr(v bool) *ListenOptions {
	lo.ReuseAddr = v
	return lo
}

func (lo *ListenOptions) WithReusePort(v bool) *ListenOptions {
	lo.ReusePort = v
	return lo
}

func (lo *ListenOptions) WithFastOpen(v int) *ListenOptions {
	lo.TCPFastOpenQueue = v
	return lo
}

func (lo *ListenOptions) WithMPTCP(v bool) *ListenOptions {
	lo.MultipathTCP = v
	return lo
}

// Inbound Helpers
func (lo *ListenOptions) WithNoDelay(v bool) *ListenOptions {
	lo.Inbounds.NoDelay = v
	return lo
}

func (lo *ListenOptions) WithBuffers(read, write int) *ListenOptions {
	lo.Inbounds.ReadBuffer = read
	lo.Inbounds.WriteBuffer = write
	return lo
}

// the same as withLinger
func (lo *ListenOptions) WithLinger(sec int) *ListenOptions {
	lo.Inbounds.DrainConnectionOnClose = sec
	return lo
}

// the same as withLinger
func (lo *ListenOptions) WithDrainConnectionOnClose(sec int) *ListenOptions {
	lo.Inbounds.DrainConnectionOnClose = sec
	return lo
}

func (lo *ListenOptions) WithDeadline(sec time.Duration) *ListenOptions {
	lo.Inbounds.Deadline = sec
	return lo
}

func (lo *ListenOptions) WithShutdownTimeout(d time.Duration) *ListenOptions {
	lo.ShutdownTimeout = d
	return lo
}

// updateConfig is an internal helper to sync ln.Options with ln.config
func (ln *Listener) updateConfig() {
	ln.config = ln.Options.convertToListenConfig()
}

func classifyAcceptError(err error) acceptErrClass {
	switch {

	case errors.Is(err, syscall.EMFILE),
		errors.Is(err, syscall.ENFILE),
		errors.Is(err, syscall.ENOMEM),
		errors.Is(err, syscall.ENOBUFS):
		return acceptErrBackoff

	case errors.Is(err, syscall.ECONNABORTED),
		errors.Is(err, syscall.EPROTO):
		return acceptErrRetryNow

	default:

		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return acceptErrBackoff
		}
		return acceptErrFatal
	}
}
