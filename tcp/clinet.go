package tcp

/*
type TcpClient struct {
	dialerOptions *net.Dialer
	Conn          net.Conn
	mu            sync.RWMutex
}

func (c *TcpClient) Connect(address string) *TcpClient {
	conn, err := c.dialerOptions.Dial("tcp", address)

	c.mu.Lock()
	c.Conn = conn
	c.mu.Unlock()

	if err != nil {
		log.Printf("[wireforge] :::: Failed to connect to forward address %s: %v", s.ForwardAddr, err)
		return nil
	}

	defer conn.Close()

	c.handler()
}

func (c *TcpClient) handler() {

}

*/
