package race

import (
	"context"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	C "github.com/metacubex/mihomo/constant"
)

type Race interface {
	Winner() C.Proxy
}

type Proxy struct {
	C.Proxy
	alts []C.Proxy
	cb   Callback
}

func (p *Proxy) checkAll(fn func(C.Proxy) bool) bool {
	if !fn(p.Proxy) {
		return false
	}
	for _, alt := range p.alts {
		if !fn(alt) {
			return false
		}
	}
	return true
}

func (p *Proxy) doDial(fn dial) (C.Conn, error) {
	if len(p.alts) == 0 {
		proxy := p.Proxy
		lat := new(Latency)
		start := time.Now()
		conn, err := fn(proxy)
		lat.Dial = time.Since(start)
		if p.cb == nil {
			return conn, err
		}
		p.cb(OnDial, lat, proxy, conn, err)
		if err != nil {
			return nil, err
		}
		//start = time.Now()
		return callback.NewFirstReadCallBackConn(conn, func(err error) {
			//lat.Read = time.Since(start)
			p.cb(OnRead, lat, proxy, conn, err)
		}), nil
	}

	proxies := make([]C.Proxy, 0, len(p.alts)+1)
	proxies = append(proxies, p.Proxy)
	proxies = append(proxies, p.alts...)
	race := newConn(p.cb, proxies...)
	err := race.doDial(fn)
	if err != nil {
		return nil, err
	}
	return race, nil
}

func (p *Proxy) Names() []string {
	names := make([]string, 0, len(p.alts)+1)
	names = append(names, p.Proxy.Name())
	for _, alt := range p.alts {
		names = append(names, alt.Name())
	}
	return names
}

func (p *Proxy) SetCallback(cb Callback) {
	p.cb = cb
}

func (p *Proxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	return p.doDial(func(p C.Proxy) (C.Conn, error) {
		return p.DialContext(ctx, metadata)
	})
}

func (p *Proxy) SupportUOT() bool {
	return p.checkAll(func(p C.Proxy) bool {
		return p.SupportUOT()
	})
}

func (p *Proxy) SupportWithDialer() C.NetWork {
	nw := p.Proxy.SupportWithDialer()
	if nw == C.UDP || nw == C.InvalidNet {
		return nw
	}
	for _, alt := range p.alts {
		anw := alt.SupportWithDialer()
		if anw != nw && anw != C.ALLNet {
			return C.InvalidNet
		}
	}
	return nw
}

func (p *Proxy) DialContextWithDialer(ctx context.Context, dialer C.Dialer, metadata *C.Metadata) (C.Conn, error) {
	return p.doDial(func(p C.Proxy) (C.Conn, error) {
		return p.DialContextWithDialer(ctx, dialer, metadata)
	})
}

func (p *Proxy) IsL3Protocol(metadata *C.Metadata) bool {
	return p.checkAll(func(p C.Proxy) bool {
		return p.IsL3Protocol(metadata)
	})
}

func (p *Proxy) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return nil
}

func (p *Proxy) Close() error {
	return nil
}

func NewProxy(proxies ...C.Proxy) C.Proxy {
	if len(proxies) < 1 {
		return nil
	}
	return &Proxy{Proxy: proxies[0], alts: proxies[1:]}
}
