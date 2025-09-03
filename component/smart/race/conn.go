package race

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/callback"
	C "github.com/metacubex/mihomo/constant"
)

const (
	OnDial = iota
	OnRead
)

type Latency struct {
	Dial time.Duration
	//Read time.Duration
}

type Callback func(on int, lat *Latency, proxy C.Proxy, conn C.Conn, err error)

var ErrNoAlive = errors.New("no alive")

func dialContext(ctx context.Context, metadata *C.Metadata, cb Callback, proxies ...C.Proxy) (C.Conn, error) {
	if len(proxies) == 0 {
		return nil, errors.New("no proxy")
	}
	if len(proxies) == 1 {
		proxy := proxies[0]
		lat := new(Latency)
		start := time.Now()
		conn, err := proxy.DialContext(ctx, metadata)
		if cb == nil {
			return conn, err
		}
		lat.Dial = time.Since(start)
		cb(OnDial, lat, proxy, conn, err)
		if err != nil {
			return nil, err
		}
		//start = time.Now() // FIXME
		return callback.NewFirstReadCallBackConn(conn, func(err error) {
			//lat.Read = time.Since(start)
			cb(OnRead, lat, proxy, conn, err)
		}), nil
	}

	race := newConn(cb, proxies...)
	err := race.doDial(func(p C.Proxy) (C.Conn, error) {
		return p.DialContext(ctx, metadata)
	})
	if err != nil {
		return nil, err
	}
	return race, nil
}

func (c *conn) Read(b []byte) (int, error) {
	if c.steady.Load() {
		return c.winner.conn.Read(b)
	}
	type result struct {
		w bool
		b []byte
		n int
	}
	res, err := c.doRead(func(won bool, conn C.Conn) (any, error) {
		var bs []byte
		if won {
			bs = b
		} else {
			bs = make([]byte, len(b), cap(b))
		}
		n, err := conn.Read(bs)
		return &result{won, bs, n}, err
	})
	r, ok := res.(*result)
	if !ok {
		return 0, err
	}
	if !r.w {
		copy(b, r.b)
	}
	return r.n, err
}

func (c *conn) ReadBuffer(b *buf.Buffer) error {
	if c.steady.Load() {
		return c.winner.conn.ReadBuffer(b)
	}
	type result struct {
		w bool
		b *buf.Buffer
	}
	rawCap := b.RawCap()
	res, err := c.doRead(func(won bool, conn C.Conn) (any, error) {
		var bu *buf.Buffer
		if won {
			bu = b
		} else {
			bu = buf.NewSize(rawCap)
		}
		return &result{won, bu}, conn.ReadBuffer(bu)
	})
	r, ok := res.(*result)
	if !ok {
		return err
	}
	if !r.w && err == nil {
		_, err = b.ReadFrom(r.b)
	}
	return err
}

func (c *conn) Write(b []byte) (int, error) {
	if c.steady.Load() {
		return c.winner.conn.Write(b)
	}
	res, err := c.doWrite(func(won bool, conn C.Conn) (any, error) {
		return conn.Write(b)
	})
	n, ok := res.(int)
	if !ok {
		return 0, err
	}
	return n, err
}

func (c *conn) WriteBuffer(b *buf.Buffer) error {
	if c.steady.Load() {
		return c.winner.conn.WriteBuffer(b)
	}
	_, err := c.doWrite(func(won bool, conn C.Conn) (any, error) {
		var bu *buf.Buffer
		if won {
			bu = b
		} else {
			bu = b.ToOwned()
		}
		return nil, conn.WriteBuffer(bu)
	})
	return err
}

func (c *conn) Close() error {
	if c.steady.Load() {
		return c.winner.close()
	}
	defer c.wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.winner != nil {
		return c.winner.close()
	}
	return c.doClose()
}

func (c *conn) LocalAddr() net.Addr {
	return c.leader.Load().conn.LocalAddr()
}

func (c *conn) RemoteAddr() net.Addr {
	return c.leader.Load().conn.RemoteAddr()
}

func (c *conn) SetDeadline(t time.Time) error {
	return errors.Join(c.SetReadDeadline(t), c.SetWriteDeadline(t))
}

func (c *conn) SetReadDeadline(t time.Time) error {
	if c.steady.Load() {
		return c.winner.conn.SetReadDeadline(t)
	}
	c.mu.Lock()
	c.rdl = t
	c.mu.Unlock()
	return nil
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	if c.steady.Load() {
		return c.winner.conn.SetWriteDeadline(t)
	}
	_, err := c.doWrite(func(won bool, conn C.Conn) (any, error) {
		return nil, conn.SetWriteDeadline(t)
	})
	return err
}

func (c *conn) Chains() C.Chain {
	if c.steady.Load() {
		return c.winner.conn.Chains()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := c.leader.Load().conn
	if c.chain == nil {
		return conn.Chains()
	}
	ccs := conn.Chains()
	ret := make(C.Chain, 0, len(c.chain)+len(ccs))
	ret = append(ret, ccs...)
	for _, a := range c.chain {
		ret = append(ret, a.Name())
	}
	return ret
}

func (c *conn) AppendToChains(a C.ProxyAdapter) {
	if c.steady.Load() {
		c.winner.conn.AppendToChains(a)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.chain != nil {
		c.chain = append(c.chain, a)
	} else {
		c.winner.conn.AppendToChains(a)
	}
}

func (c *conn) RemoteDestination() string {
	return c.leader.Load().conn.RemoteDestination()
}

func (c *conn) Upstream() any {
	return c.leader.Load().conn
}

func (c *conn) WriterReplaceable() bool {
	return c.steady.Load()
}

func (c *conn) ReaderReplaceable() bool {
	return c.steady.Load()
}

func (c *conn) Winner() C.Proxy {
	return c.leader.Load().proxy
}
