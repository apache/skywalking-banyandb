package bytes

import (
	"sync"
)

var defaultPool Pool

type Pool struct {
	p sync.Pool
}

func Borrow() *ByteBuf {
	return defaultPool.Borrow()
}

func (p *Pool) Borrow() *ByteBuf {
	v := p.p.Get()
	if v != nil {
		return v.(*ByteBuf)
	}
	return &ByteBuf{
		b: make([]byte, 0),
	}
}

func Return(buf *ByteBuf) {
	buf.Reset()
	defaultPool.Return(buf)
}

func (p *Pool) Return(buf *ByteBuf) {
	p.p.Put(buf)
}
