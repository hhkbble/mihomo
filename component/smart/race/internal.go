package race

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

var racerPool = sync.Pool{
	New: func() interface{} {
		return new(racer)
	},
}

type command func(won bool, conn C.Conn) (any, error)
type dial func(C.Proxy) (C.Conn, error)

type racer struct {
	Latency
	conn  C.Conn
	proxy C.Proxy
	ready chan struct{}
	alive atomic.Bool
	done  chan struct{}
}

func (racer *racer) close() error {
	if !racer.alive.Load() {
		return nil
	}
	racer.alive.Store(false)
	return racer.conn.Close()
}

type conn struct {
	racers []*racer
	once   sync.Once // read
	rdl    time.Time
	cb     Callback
	mu     sync.Mutex
	cond   sync.Cond
	writes []command
	wg     sync.WaitGroup // write
	leader atomic.Pointer[racer]
	winner *racer
	steady atomic.Bool
	chain  []C.ProxyAdapter
}

func newConn(cb Callback, proxies ...C.Proxy) *conn {
	ret := new(conn)
	ret.racers = make([]*racer, len(proxies))
	ret.cb = cb
	ret.writes = make([]command, 0, 1)
	ret.chain = make([]C.ProxyAdapter, 0, 1)
	ret.cond.L = &ret.mu
	for i := range ret.racers {
		r := racerPool.Get().(*racer)
		ret.racers[i] = r
		r.Latency = Latency{}
		r.conn = nil
		r.proxy = proxies[i]
		r.ready = make(chan struct{})
		r.alive.Store(false)
		r.done = make(chan struct{})
	}
	return ret
}

func (c *conn) doDial(fn dial) error {
	once := sync.Once{}
	ok := false
	wg := sync.WaitGroup{}
	done := make(chan struct{})
	errs := make([]error, len(c.racers))

	wg.Add(len(c.racers))
	go func() {
		wg.Wait()
		once.Do(func() {
			close(done)
		})
	}()

	for i := range c.racers {
		n := i
		r := c.racers[n]
		go func() {
			defer wg.Done()
			defer close(r.ready)
			start := time.Now()
			res, err := fn(r.proxy)
			r.Latency.Dial = time.Since(start)
			if c.cb != nil {
				c.cb(OnDial, &r.Latency, r.proxy, res, err)
			}
			if err == nil {
				r.conn = res
				r.alive.Store(true)
				once.Do(func() {
					c.leader.Store(r) // always not nil
					ok = true
					close(done)
				})
			} else {
				errs[n] = err
			}
		}()
	}

	c.goWrite()

	<-done
	if ok {
		return nil
	}
	return joinOrNoAlive(errs...)
}

func (c *conn) goWrite() {
	c.wg.Add(len(c.racers))
	for i := range c.racers {
		r := c.racers[i]
		go func() {
			defer c.wg.Done()
			defer close(r.done)

			pos := 0
			wait := func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				for {
					if !r.alive.Load() {
						return false
					}
					if c.winner == nil {
						if pos < len(c.writes) {
							return true
						}
						c.cond.Wait()
					} else {
						if c.winner == r {
							return pos < len(c.writes)
						}
						_ = r.close()
						return false
					}
				}
			}

			<-r.ready
			for {
				if !wait() {
					return
				}
				c.mu.Lock()
				cmd := c.writes[pos]
				c.mu.Unlock()
				_, err := cmd(false, r.conn)
				pos++
				if err != nil {
					_ = r.close()
					return
				}
			}
		}()
	}
}

func (c *conn) doWrite(cmd command) (any, error) {
	c.mu.Lock()
	if c.winner != nil {
		c.mu.Unlock()
		<-c.winner.done
		c.steady.Store(true)
		return cmd(true, c.winner.conn)
	}

	var wres any
	once := sync.Once{}
	ok := false
	done := make(chan struct{})
	errs := make([]error, len(c.racers))

	go func() {
		c.wg.Wait() // since write loop breaks on any error
		once.Do(func() {
			close(done)
		})
	}()

	var n int32 = 0
	c.writes = append(c.writes, func(win bool, conn C.Conn) (any, error) {
		res, err := cmd(win, conn)
		if err == nil {
			once.Do(func() {
				wres = res
				ok = true
				close(done)
			})
		} else {
			errs[atomic.AddInt32(&n, 1)-1] = err
		}
		return res, err
	})
	c.cond.Broadcast()
	c.mu.Unlock()

	<-done
	if ok {
		return wres, nil
	}
	return nil, joinOrNoAlive(errs...)
}

func (c *conn) doRead(cmd command) (any, error) {
	var rres any
	var rerr error
	did := false
	c.mu.Lock()
	rdl := c.rdl
	c.mu.Unlock()
	c.once.Do(func() {
		did = true

		once := sync.Once{}
		ok := false
		wg := sync.WaitGroup{}
		done := make(chan struct{})
		errs := make([]error, len(c.racers))

		wg.Add(len(c.racers))
		go func() {
			wg.Wait()
			once.Do(func() {
				close(done)
			})
			c.wg.Wait()
			c.mu.Lock()
			if c.winner != nil {
				for _, r := range c.racers {
					if r != c.winner {
						racerPool.Put(r)
					}
				}
				// for gc
				c.racers = nil
				c.writes = nil
				c.cb = nil
			}
			c.mu.Unlock()
		}()

		for i := range c.racers {
			n := i
			r := c.racers[n]

			go func() {
				defer wg.Done()
				<-r.ready
				if !r.alive.Load() {
					return
				}
				lead := false
				defer func() {
					if !lead {
						_ = r.close()
					}
				}()
				c.mu.Lock()
				winner := c.winner
				c.mu.Unlock()
				if winner != nil {
					return
				}
				if !rdl.IsZero() {
					err := r.conn.SetReadDeadline(rdl)
					if err != nil {
						errs[n] = err
						return
					}
				}
				//start := time.Now()
				res, err := cmd(false, r.conn)
				//r.Latency.Read = time.Since(start)
				if c.cb != nil {
					c.cb(OnRead, &r.Latency, r.proxy, r.conn, err)
				}
				if err == nil {
					once.Do(func() {
						lead = true
						rres = res
						c.mu.Lock()
						c.leader.Store(r)
						c.winner = r
						for _, a := range c.chain {
							r.conn.AppendToChains(a)
						}
						c.chain = nil
						c.cond.Broadcast()
						c.mu.Unlock()
						ok = true
						close(done)
					})
				} else {
					errs[n] = err
				}
			}()
		}

		<-done
		if !ok {
			rerr = joinOrNoAlive(errs...)
		}
	})

	if did {
		return rres, rerr
	}

	if c.winner == nil { // without lock since it won't change anymore
		return nil, ErrNoAlive
	}

	if !rdl.IsZero() {
		err := c.winner.conn.SetReadDeadline(rdl)
		if err != nil {
			_ = c.winner.conn.Close()
			return nil, err
		}
	}

	return cmd(true, c.winner.conn)
}

func (c *conn) doClose() error {
	errs := make([]error, len(c.racers))
	wg := sync.WaitGroup{}
	wg.Add(len(c.racers))
	for i := range c.racers {
		n := i
		r := c.racers[n]
		go func() {
			defer wg.Done()
			<-r.ready
			errs[n] = r.close()
		}()
	}
	wg.Wait()
	c.cond.Broadcast()
	return errors.Join(errs...)
}

func joinOrNoAlive(errs ...error) error {
	err := errors.Join(errs...)
	if err == nil {
		return ErrNoAlive
	}
	return err
}
