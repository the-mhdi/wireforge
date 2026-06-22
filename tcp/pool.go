package tcp

import "sync"

func BufferPool(size uint32) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return make([]byte, size)
		},
	}
}
