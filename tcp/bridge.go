package tcp

import (
	"io"
	"net"
	"sync"
)

const BRIDGE_BUFFER_SIZE = 128 * 1024 // 128KB

var (
	lnPool = sync.Pool{
		New: func() any {
			return make([]byte, BRIDGE_BUFFER_SIZE)
		},
	}
)

func Bridge(src, dst net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// conn1 -> conn2
	go func() {
		defer wg.Done()
		buf := lnPool.Get().([]byte)
		defer lnPool.Put(buf)

		io.CopyBuffer(dst, src, buf)
		//TCP Half-Close = Close the write half of the destination to forward the EOF (TCP FIN) // to managed short lived conns
		if tcpDst, ok := dst.(*net.TCPConn); ok {
			tcpDst.CloseWrite()
		}

		//log.Printf("[wireforge] :::: CLOSING Accepted Connection From %s, Reason %v", src.RemoteAddr().String(), err)
	}()

	// conn2 -> conn1
	go func() {
		defer wg.Done()
		buf := lnPool.Get().([]byte)
		defer lnPool.Put(buf)

		io.CopyBuffer(src, dst, buf)

		if tcpSrc, ok := src.(*net.TCPConn); ok {
			tcpSrc.CloseWrite()
		}

		//log.Printf("[wireforge] :::: CLOSING Forward Connection to %s, Reason %v ", dst.LocalAddr().String(), err)
	}()

	// Wait for both sides to finish their transfers
	wg.Wait()
}

// same as Bridge() but retruns the number of bytes written and read
func BridgeWithMetric(src, dst net.Conn) (sent int64, received int64) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Stream: src -> dst
	go func() {
		defer wg.Done()
		buf := lnPool.Get().([]byte)
		defer lnPool.Put(buf)

		sent, _ = io.CopyBuffer(dst, src, buf)

		if tcpDst, ok := dst.(*net.TCPConn); ok {
			tcpDst.CloseWrite()
		}

	}()

	// Stream: dst -> src
	go func() {
		defer wg.Done()
		buf := lnPool.Get().([]byte)
		defer lnPool.Put(buf)

		received, _ = io.CopyBuffer(src, dst, buf)

		if tcpSrc, ok := src.(*net.TCPConn); ok {
			tcpSrc.CloseWrite()
		}
	}()

	// Wait for both sides to finish their transfers
	wg.Wait()

	return sent, received
}
