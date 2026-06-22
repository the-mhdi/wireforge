package tcp

import (
	"context"
	"log"
	"net"
)

type Forwarder struct {
	ForwardAddr string
}

func (s *Forwarder) handle(ctx context.Context, conn net.Conn) {
	dst, err := net.Dial("tcp", s.ForwardAddr)

	if err != nil {
		log.Printf("[wireforge] :::: Failed to connect to forward address %s: %v", s.ForwardAddr, err)
		return
	}

	defer dst.Close()

	log.Printf("[wireforge] :::: Connected to forward address %s", s.ForwardAddr)

	// Start bidirectional copy
	Bridge(conn, dst)
	// conn = inbound --- dst = outbound
}
