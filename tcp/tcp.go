package tcp

import (
	"net"
)

func Listen(addr string, handler handlerFunc) error {
	ln := NewListener(DefaultListenOptions())
	err := ln.Initialize(addr, handler)
	if err != nil {
		return err
	}

	ln.Run()

	return nil
}

func Dial(addr string) (net.Conn, error) {
	dl := DefaultDialOptions().NewDialer()
	return dl.Dial(addr)
}

/*
// chekcs if the connection gets closed, it redilas automatically, it is useful for long lived connections like tunnels or reverse proxies
func DialPersistent(addr string) <-chan net.Conn {

	opts := DefaultDialOptions().WithKeepAlive(true)
	dl := opts.NewDialer()

	conn, _ := dl.Dial(addr)

	ch := make(chan net.Conn, 1)
	ch <- conn

	go func(dialer *Dialer) {
		for {
			select {

			case <-dl.Closed:
				log.Printf("Connection to %s lost, attempting to reconnect...", addr)
				conn, _ := dl.Dial(addr)
				ch <- conn
				log.Printf("Successfully reconnected to %s", addr)

			default:
				conn, _ := dl.Dial(addr)
				ch <- conn
			}
		}
	}(dl)

	return ch
}
*/
/* api

wireforge tcp [server or daemon] listen ip:port auth [password/publickey] directory - or instead of these wireforge tcp server config [config_dir]

wireforge tcp [server or daemon] listen ip:port
wireforge shell [username@daemonip:port] or wireforge tcp client connect [daemonip:port] user [username]

wireforge tcp connect [daemonip:port] forward remote [ip:port] to local [ip:port]

wireforge tcp connect [daemonip:port] forward local [ip:port] to remote [ip:port]

wireforge tcp forward local [local or this machine ip : port (listens to upcomming tcp traffic to this port)] to remote [a remote public ip of another tcp server or service]  //reverse proxy server

// if auth is pass user will be asked to inter username and password it is unsafe, later with implementing tls i'll imporove this
*/
