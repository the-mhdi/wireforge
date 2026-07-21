package tcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type handlerFunc func(ctx context.Context, conn net.Conn)
type OnConnectFunc func(ctx context.Context, conn net.Conn) error // Called on connection accept
type OnDisconnectFunc func(ctx context.Context, conn net.Conn)    // Called on disconnect

type Listener struct {
	//todo -> max connection allowed
	Address     string
	isListening bool
	listener    net.Listener
	config      *net.ListenConfig
	Options     *ListenOptions

	AcceptPool sync.Map //map of connections that have been accepted by the listener but not yet completed the OnConnect task

	ConnectionPool sync.Map //map of active connections that have passed the onconnect task
	mu             sync.RWMutex
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	ErrCh          chan error

	connCountMu sync.RWMutex
	connCount   map[net.Addr]uint
}

type ListenOptions struct {
	Verbose bool

	ReuseAddr    bool // Bypass TIME_WAIT on restart
	ReusePort    bool // Leave false by default unless specifically needed, Allow multiple listeners on the same port for multi-core scaling
	TCPFastOpen  bool
	MultipathTCP bool //only works if OS supports

	//events
	//Connection Hooks

	OnConnect func(context.Context, net.Conn) error //executes before handler gets called, after connection gets accepted and before being added to the pool, nil by deault.

	OnConnectTimeout time.Duration // closes the conncetion when timeout reached // default is 0 = no timeout

	OnDisconnect func(context.Context, net.Conn) //executes after connection closes and deleted from the pool, nil by deault, Cleanup hook

	OnDisconnectTimeout time.Duration //default is 0

	//the max time the Stop() method can try closing and draining the remaining connections for a graceful shutdown
	ShutdownTimeout time.Duration //default 15s

	//incomming connections configuration
	Inbounds InboundConnOptions

	MaxConnectionsPerIP uint // 0 == no limit
}

// keepAlive options on ListenOptions gets applied to all Incoming conncetions
type InboundConnOptions struct {
	NoDelay     bool //default = true (if os supports)
	WriteBuffer int  // default is 0 = Let OS Auto-Tune dynamically
	ReadBuffer  int  // default is 0 = Let OS Auto-Tune dynamically

	Deadline time.Duration //== absolute deadline , default is 0 == no timeout //the absolute time that server lives

	DrainConnectionOnClose int // default -1 // == Linger//by default (-1) the operating system finishes sending the data in the background

	KeepAlive bool // Time between Keep-Alive probes

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
		ConnectionPool: sync.Map{},
		config:         lo.convertToListenConfig(),
		ErrCh:          make(chan error, 3),
		ctx:            ctx,
		cancel:         cancel,
		connCount:      make(map[net.Addr]uint),
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

	ln.Run()

}

// listen method here is a  blocking call that automatocally handels the life cycle of tcp listener
// for more control over the life cycle of your listener see NewListener() and listener.Initialize() and Run() method
func (lo *ListenOptions) Listen(addr string, handler func(context.Context, net.Conn)) {
	lo.ListenWithContext(context.Background(), addr, handler)

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

			case ln.ErrCh <- fmt.Errorf("Accept Failure - retry in %v: %v", tempDelay, err):

			default:
				// If channel is full, we log to stderr so we don't lose the error
				log.Printf("[wireforge] Error channel full, dropped error: %v", err)
			}

			// Handle temporary errors (like running out of File Descriptors)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				if ln.Options.Verbose {
					log.Printf("[wireforge] :::: Listener Accept Loop Error: %v; retrying in %v", err, tempDelay)
				}
				time.Sleep(tempDelay)
				continue
			}

			// For fatal errors (like listener closed), exit the loop

			log.Printf("[Log] Exiting Listener Accept Loop : %v", err)

			return
		}

		tempDelay = 0 // Reset delay on success

		// Post-Accept Tuning
		if tcpConn, ok := inbound.(*net.TCPConn); ok {

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
			if ln.Options.MaxConnectionsPerIP != 0 && ln.incrementConnCount(tcpConn.RemoteAddr()) > ln.Options.MaxConnectionsPerIP {
				log.Printf("[wireforge] :::: reached max number of connections for this ip %s", tcpConn.RemoteAddr().String())
				inbound.Close()
			}

		}

		ln.wg.Add(1)
		go func(lsn *Listener, c net.Conn, handlerFunc func(context.Context, net.Conn)) {
			defer lsn.wg.Done()

			defer c.Close()

			defer func() {
				if r := recover(); r != nil {
					lsn.sendError(fmt.Errorf("handler panic for %s: %v", c.RemoteAddr(), r))
					if lsn.Options.Verbose {
						log.Printf("[wireforge] PANIC RECOVERED: %v\n%s", r, debug.Stack())
					}
				}
			}()

			// Add connection to pool
			lsn.AcceptPool.Store(c, struct{}{})
			defer lsn.AcceptPool.Delete(c)

			if lsn.Options.Verbose {
				log.Printf("[wireforge] :::: New CONNECTION [ %s ] ACCEPTED by the Listener [ %s ] ", c.RemoteAddr().String(), lsn.Address)
			}

			//OnConnect() function called on connection accept
			if lsn.Options.OnConnect != nil {

				if lsn.Options.OnConnectTimeout > 0 {
					// Create a specific sub-context for this timeout hook
					cCtx, cancel := context.WithTimeout(lsn.ctx, lsn.Options.OnConnectTimeout)

					resCh := make(chan error, 1)

					// 2. Call onconnect with explicit parameter passing
					go func(ctx context.Context, conn net.Conn) {

						//panic recovery
						defer func() {
							if r := recover(); r != nil {
								lsn.sendError(fmt.Errorf("onConnect panic recovered: %v", r))
							}
						}()

						//call the user's onConnect function implementation
						resCh <- lsn.Options.OnConnect(ctx, conn)
					}(cCtx, c)

					select {
					case <-cCtx.Done():
						// The timer ran out!
						cancel()
						lsn.sendError(fmt.Errorf("onConnect timed out for %s", c.RemoteAddr()))
						return // Connection closes by defer

					case err := <-resCh:
						// The hook finished in time
						cancel()

						if err != nil {
							lsn.sendError(fmt.Errorf("onConnect rejected %s: %w", c.RemoteAddr(), err))
							return
						}
					}

				}
				//if no timeout is set for onconnection()
				if lsn.Options.OnConnectTimeout == 0 {

					err := lsn.Options.OnConnect(lsn.ctx, c)

					if err != nil {
						lsn.sendError(fmt.Errorf("onConnect rejected %s: %w", c.RemoteAddr(), err))
						return
					}
				}

				//log successful onconnect execution
				if lsn.Options.Verbose {
					log.Printf("[wireforge] :::: OnConnect Function Executed Successfully")
				}

			}

			lsn.AcceptPool.Delete(c)

			lsn.ConnectionPool.Store(c, struct{}{})
			defer lsn.ConnectionPool.Delete(c)

			//onDisconnect being here ensures that it only fires if onConnect has been successful,
			// if this go routine is returned at onConnect (if the connection is rejected by onConnect), the onDisconnect logic won't be executed
			defer func() {
				if lsn.Options.OnDisconnect != nil {

					if lsn.Options.OnDisconnectTimeout > 0 {
						dCtx, dCancel := context.WithTimeout(lsn.ctx, lsn.Options.OnDisconnectTimeout)
						defer dCancel()
						lsn.Options.OnDisconnect(dCtx, c)
					} else {
						lsn.Options.OnDisconnect(lsn.ctx, c)
					}
				}
			}()

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

func (ln *Listener) incrementConnCount(addr net.Addr) (currentCount uint) {
	ln.connCountMu.Lock()
	defer ln.connCountMu.Unlock()
	if _, ok := ln.connCount[addr]; ok {
		ln.connCount[addr]++
		return ln.connCount[addr]

	} else {
		ln.connCount[addr] = 1
		return ln.connCount[addr]
	}
	//return ln.connCount.Add(1)
}

// just a blocking call waiting for the main listener to be shutdown
func (ln *Listener) Run() {
	//TO_DO : after sig term, "no use of closed network connection" should be caught and loged
	go func() {
		for err := range ln.ErrCh {
			if ln.Options.Verbose {
				log.Println(err)
			}
		}
	}()

	log.Printf("[wireforge] TCP LISTENER STARTED SUCCESSFULLY, Listening on %s", ln.Address)

	quit := make(chan os.Signal, 1)
	// Catch Ctrl+C (SIGINT) and Docker/K8s stop (SIGTERM)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {

	case <-quit:
		log.Printf(" SIGTERM CAUGHT: tcp listener [%s] is being stoped ", ln.Address)
	case <-ln.ctx.Done():
		log.Printf("Shutting down listener [%s] gracefully...", ln.Address)
	}

	done := make(chan struct{})

	go func() {
		err := ln.Stop(ln.Options.ShutdownTimeout)
		if err != nil {
			log.Printf("Error during listener [%s] shutdown: %v", ln.Address, err)
		}
		close(done)
	}()

	<-done

	log.Printf("TCP Listener [%s] successfully stopped. All connections closed.", ln.Address)

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

	defer close(ln.ErrCh) // Close error channel to signal no more errors will be sent

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

		// Optional: Actively loop through your sync.Map and aggressively close stuck connections
		ln.ConnectionPool.Range(func(inbounds, outbounds any) bool {
			if c, ok := inbounds.(net.Conn); ok {
				c.Close() // Forcefully drop the client
			}
			if b, ok := outbounds.(net.Conn); b != nil && ok {
				b.Close() // Forcefully drop the backend
			}
			return true
		})

		ln.AcceptPool.Range(func(key, value any) bool {
			if c, ok := key.(net.Conn); ok {
				c.Close()
			}
			return true
		})

		return nil
	}
}

func (ln *Listener) sendError(err error) {
	select {
	case ln.ErrCh <- err:
	default:
		if ln.Options.Verbose {
			log.Printf("[wireforge] Error channel full, dropping error: %v", err)
		}
	}
}

func (ln *Listener) OnConnect(onConn OnConnectFunc) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnConnect = onConn
}

func (ln *Listener) OnDisconnect(onDisconn OnDisconnectFunc) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnDisconnect = onDisconn
}

func (ln *Listener) OnDisconnectWithTimeout(d time.Duration, onDisconn OnDisconnectFunc) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnDisconnectTimeout = d
	ln.Options.OnDisconnect = onDisconn
}

func (ln *Listener) OnConnectWithTimeout(d time.Duration, onConn OnConnectFunc) {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnConnectTimeout = d
	ln.Options.OnConnect = onConn
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
				if lo.TCPFastOpen {
					if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, unix.TCP_FASTOPEN, 1); err != nil {
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
		TCPFastOpen:         false,
		MultipathTCP:        true,
		Inbounds:            inboundOps,
		OnConnect:           nil,
		OnConnectTimeout:    0,
		OnDisconnect:        nil,
		OnDisconnectTimeout: 0,
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

func (lo *ListenOptions) WithFastOpen(v bool) *ListenOptions {
	lo.TCPFastOpen = v
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

func (lo *ListenOptions) WithOnConnectTimeout(sec time.Duration) *ListenOptions {
	lo.OnConnectTimeout = sec
	return lo
}

func (lo *ListenOptions) WithOnDisconnectTimeout(sec time.Duration) *ListenOptions {
	lo.OnDisconnectTimeout = sec
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

func (ln *Listener) SetVerbose(v bool) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.Verbose = v
	return ln
}

func (ln *Listener) SetKeepAlive(v bool) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.isListening {
		if ln.Options.Verbose {
			log.Println("[wireforge] WARNING: KeepAlive changes won't affect already-accepted connections")
		}
	}
	ln.Options.Inbounds.KeepAlive = v
	ln.updateConfig()
	return ln
}
func (ln *Listener) SetKeepAliveFirstProbe(sec time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.isListening {
		if ln.Options.Verbose {
			log.Println("[wireforge] WARNING: KeepAlive changes won't affect already-accepted connections")
		}
	}
	ln.Options.Inbounds.KeepAliveFirstProbe = sec
	ln.updateConfig()
	return ln
}

func (ln *Listener) SetKeepAliveInterval(sec time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.isListening {
		if ln.Options.Verbose {
			log.Println("[wireforge] WARNING: KeepAlive changes won't affect already-accepted connections")
		}
	}
	ln.Options.Inbounds.KeepAliveInterval = sec
	ln.updateConfig()
	return ln
}

func (ln *Listener) SetMaxKeepAliveAttempts(count int) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.isListening {
		if ln.Options.Verbose {
			log.Println("[wireforge] WARNING: KeepAlive changes won't affect already-accepted connections")
		}
	}
	ln.Options.Inbounds.MaxKeepAliveAttempts = count
	ln.updateConfig()
	return ln
}

func (ln *Listener) SetReusePort(v bool) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.ReusePort = v
	ln.updateConfig()
	return ln
}

func (ln *Listener) SetMPTCP(v bool) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.MultipathTCP = v
	ln.updateConfig()
	return ln
}

// SetInboundOptions allows updating all connection-specific settings at once
func (ln *Listener) SetInboundOptions(noDelay bool, rBuf, wBuf, linger int, deadline time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.Inbounds.NoDelay = noDelay
	ln.Options.Inbounds.ReadBuffer = rBuf
	ln.Options.Inbounds.WriteBuffer = wBuf
	ln.Options.Inbounds.DrainConnectionOnClose = linger
	ln.Options.Inbounds.Deadline = deadline
	return ln
}

func (ln *Listener) SetDeadline(d time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.Inbounds.Deadline = d
	return ln
}

func (ln *Listener) SetLinger(sec int) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.Inbounds.DrainConnectionOnClose = sec
	return ln
}

func (ln *Listener) SetDrainConnectionOnClose(sec int) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.Inbounds.DrainConnectionOnClose = sec
	return ln
}

func (ln *Listener) SetOnConnectTimeout(d time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnConnectTimeout = d
	return ln
}

func (ln *Listener) SetOnDisconnectTimeout(d time.Duration) *Listener {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	ln.Options.OnDisconnectTimeout = d
	return ln
}
