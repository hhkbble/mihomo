package race

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

// mockConn is a C.Conn implementation that records whether it is closed.
type mockConn struct {
	net.Conn
	closed   atomic.Bool
	closedCh chan struct{}
	chain    C.Chain
}

func newMockConn() (*mockConn, net.Conn) {
	c1, c2 := net.Pipe()
	return &mockConn{Conn: c1, closedCh: make(chan struct{})}, c2
}

func (m *mockConn) Close() error {
	if m.closed.CompareAndSwap(false, true) {
		close(m.closedCh)
	}
	return m.Conn.Close()
}

func (m *mockConn) ReadBuffer(b *buf.Buffer) error {
	_, err := b.ReadOnceFrom(m.Conn)
	return err
}

func (m *mockConn) WriteBuffer(b *buf.Buffer) error {
	_, err := b.WriteTo(m.Conn)
	return err
}

func (m *mockConn) Chains() C.Chain                 { return m.chain }
func (m *mockConn) AppendToChains(a C.ProxyAdapter) { m.chain = append(m.chain, a.Name()) }
func (m *mockConn) RemoteDestination() string       { return "" }
func (m *mockConn) Upstream() any                   { return m.Conn }

// mockProxy implements C.Proxy with controllable dial behaviour.
type mockProxy struct {
	name  string
	delay time.Duration
	conn  *mockConn
	err   error
}

func (m *mockProxy) Adapter() C.ProxyAdapter                      { return m }
func (m *mockProxy) AliveForTestUrl(string) bool                  { return true }
func (m *mockProxy) DelayHistory() []C.DelayHistory               { return nil }
func (m *mockProxy) ExtraDelayHistories() map[string]C.ProxyState { return nil }
func (m *mockProxy) LastDelayForTestUrl(string) uint16            { return 0 }
func (m *mockProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	return 0, nil
}
func (m *mockProxy) StatusTest(context.Context, string, utils.IntRanges[uint16]) (uint16, bool, error) {
	return 0, true, nil
}
func (m *mockProxy) Dial(*C.Metadata) (C.Conn, error) {
	return m.DialContext(context.Background(), nil)
}
func (m *mockProxy) DialUDP(*C.Metadata) (C.PacketConn, error) {
	return nil, errors.New("not implemented")
}

func (m *mockProxy) Name() string                 { return m.name }
func (m *mockProxy) Type() C.AdapterType          { return 0 }
func (m *mockProxy) Addr() string                 { return "" }
func (m *mockProxy) SupportUDP() bool             { return false }
func (m *mockProxy) ProxyInfo() C.ProxyInfo       { return C.ProxyInfo{} }
func (m *mockProxy) MarshalJSON() ([]byte, error) { return []byte("{}"), nil }
func (m *mockProxy) StreamConnContext(ctx context.Context, c net.Conn, _ *C.Metadata) (net.Conn, error) {
	return c, nil
}
func (m *mockProxy) DialContext(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.conn, nil
}
func (m *mockProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, errors.New("not implemented")
}
func (m *mockProxy) SupportUOT() bool             { return false }
func (m *mockProxy) SupportWithDialer() C.NetWork { return 0 }
func (m *mockProxy) DialContextWithDialer(ctx context.Context, _ C.Dialer, md *C.Metadata) (C.Conn, error) {
	return m.DialContext(ctx, md)
}
func (m *mockProxy) ListenPacketWithDialer(context.Context, C.Dialer, *C.Metadata) (C.PacketConn, error) {
	return nil, errors.New("not implemented")
}
func (m *mockProxy) IsL3Protocol(*C.Metadata) bool    { return false }
func (m *mockProxy) Unwrap(*C.Metadata, bool) C.Proxy { return m }
func (m *mockProxy) Close() error                     { return nil }

// Callback recorder

type cbRecorder struct {
	mu    sync.Mutex
	calls map[int][]C.Proxy
}

func newCbRecorder() *cbRecorder {
	return &cbRecorder{calls: make(map[int][]C.Proxy)}
}

func (c *cbRecorder) CB(on int, _ *Latency, p C.Proxy, _ C.Conn, err error) {
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[on] = append(c.calls[on], p)
}

// --- Tests ---

func TestDial_SingleProxy(t *testing.T) {
	mc, _ := newMockConn()
	p1 := &mockProxy{name: "p1", conn: mc}
	cb := newCbRecorder()

	rcConn, err := dialContext(context.Background(), nil, cb.CB, p1)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()
	_, ok := rcConn.(*conn)
	if ok {
		t.Fatalf("expected firstReadCallBackConn, got conn")
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.calls[OnDial]) != 1 {
		t.Fatalf("expected 1 OnDial call, got %d", len(cb.calls[OnDial]))
	}
	if cb.calls[OnDial][0] != p1 {
		t.Fatalf("expected OnDial call for %s, got %s", p1.name, cb.calls[OnDial][0].Name())
	}
}

func TestDial_SelectsFastest(t *testing.T) {
	mc1, _ := newMockConn()
	mc2, _ := newMockConn()
	p1 := &mockProxy{name: "p1", delay: 50 * time.Millisecond, conn: mc1}
	p2 := &mockProxy{name: "p2", delay: 10 * time.Millisecond, conn: mc2}
	p3 := &mockProxy{name: "p3", err: errors.New("fail")}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2, p3)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()
	rc := rcConn.(*conn)
	if rc.leader.Load().proxy != p2 {
		t.Errorf("expected leader %s, got %s", p2.name, rc.leader.Load().proxy.(*mockProxy).name)
	}
}

func TestDial_AllFail(t *testing.T) {
	p1 := &mockProxy{name: "p1", err: errors.New("fail1")}
	p2 := &mockProxy{name: "p2", err: errors.New("fail2")}

	_, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestDial_ContextCancel(t *testing.T) {
	mc1, _ := newMockConn()
	mc2, _ := newMockConn()
	p1 := &mockProxy{name: "p1", delay: 200 * time.Millisecond, conn: mc1}
	p2 := &mockProxy{name: "p2", delay: 100 * time.Millisecond, conn: mc2}
	p3 := &mockProxy{name: "p3", err: errors.New("fail")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := dialContext(ctx, nil, nil, p1, p2, p3)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestRead_SelectsWinnerAndClosesLosers(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}
	cb := newCbRecorder()

	rcConn, err := dialContext(context.Background(), nil, cb.CB, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()
	rc := rcConn.(*conn)

	go func() {
		time.Sleep(30 * time.Millisecond)
		_, _ = srv1.Write([]byte("slow"))
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		_, _ = srv2.Write([]byte("fast"))
	}()

	buf := make([]byte, 4)
	n, err := rc.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf[:n]) != "fast" {
		t.Fatalf("unexpected read %q", string(buf[:n]))
	}
	if rc.winner.proxy != p2 {
		t.Fatalf("expected winner %s, got %s", p2.name, rc.winner.proxy.(*mockProxy).name)
	}

	// check loser is closed
	select {
	case <-mc1.closedCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("loser connection was not closed")
	}

	// check winner is not closed
	select {
	case <-mc2.closedCh:
		t.Fatal("winner connection was closed")
	default:
		// expected
	}

	// check callback
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if len(cb.calls[OnRead]) == 0 {
		t.Fatal("OnRead callback was not called")
	}
}

func TestRead_LeaderAndWinnerCanDiffer(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", delay: 10 * time.Millisecond, conn: mc1}
	p2 := &mockProxy{name: "p2", delay: 50 * time.Millisecond, conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()
	rc := rcConn.(*conn)

	// p1 dials faster, should be initial leader
	if rc.leader.Load().proxy != p1 {
		t.Fatalf("expected initial leader to be %s, but got %s", p1.name, rc.leader.Load().proxy.(*mockProxy).name)
	}

	// p2 responds to read faster
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = srv1.Write([]byte("slow"))
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = srv2.Write([]byte("fast"))
	}()

	buf := make([]byte, 4)
	n, err := rc.Read(buf)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(buf[:n]) != "fast" {
		t.Fatalf("unexpected read %q", string(buf[:n]))
	}

	// p2 should be the winner, and the new leader
	if rc.winner.proxy != p2 {
		t.Fatalf("expected winner %s, got %s", p2.name, rc.winner.proxy.(*mockProxy).name)
	}
	if rc.leader.Load().proxy != p2 {
		t.Fatalf("expected final leader %s, got %s", p2.name, rc.leader.Load().proxy.(*mockProxy).name)
	}

	// check initial leader connection is closed
	select {
	case <-mc1.closedCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("loser connection was not closed")
	}
}

func TestRead_AllFail(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Close server ends to cause read errors
	srv1.Close()
	srv2.Close()

	buf := make([]byte, 4)
	_, err = rcConn.Read(buf)
	if err == nil {
		t.Fatal("expected error on read, got nil")
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	_, err = rcConn.Read(buf)
	if !errors.Is(err, ErrNoAlive) {
		t.Fatalf("expected ErrNoAlive, got %v", err)
	}
}

func TestWrite_BroadcastsBeforeWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	msg := []byte("hi")
	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)
	go func() { b := make([]byte, len(msg)); _, _ = srv1.Read(b); ch1 <- string(b) }()
	go func() { b := make([]byte, len(msg)); _, _ = srv2.Read(b); ch2 <- string(b) }()

	if _, err = rcConn.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	select {
	case s := <-ch1:
		if s != string(msg) {
			t.Errorf("conn1 got %q, want %q", s, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for conn1")
	}
	select {
	case s := <-ch2:
		if s != string(msg) {
			t.Errorf("conn2 got %q, want %q", s, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for conn2")
	}
}

func TestWrite_OnlyToWinnerAfterSelection(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Make p2 the winner
	go func() { _, _ = srv2.Write([]byte("win")) }()
	buf := make([]byte, 3)
	if _, err = rcConn.Read(buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Loser should be closed by Read
	<-mc1.closedCh

	// Write after winner is selected
	msg := []byte("onlywinner")
	ch2 := make(chan string, 1)
	go func() { b := make([]byte, len(msg)); _, _ = srv2.Read(b); ch2 <- string(b) }()

	if _, err = rcConn.Write(msg); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	select {
	case s := <-ch2:
		if s != string(msg) {
			t.Fatalf("winner got %q, want %q", s, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for winner")
	}

	// Verify loser did not get the message
	n, err := srv1.Read(make([]byte, 1))
	if n > 0 || !errors.Is(err, io.EOF) {
		t.Errorf("loser connection received data or was not closed")
	}
}

func TestBufferMethods(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()
	rc := rcConn.(*conn)

	// Test WriteBuffer broadcast
	msg1 := buf.New()
	msg1.WriteString("buffer-write")
	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)
	go func() { b := make([]byte, 12); _, _ = srv1.Read(b); ch1 <- string(b) }()
	go func() { b := make([]byte, 12); _, _ = srv2.Read(b); ch2 <- string(b) }()
	if err = rc.WriteBuffer(msg1); err != nil {
		t.Fatalf("WriteBuffer failed: %v", err)
	}
	if s := <-ch1; s != "buffer-write" {
		t.Errorf("conn1 read with WriteBuffer got %q", s)
	}
	if s := <-ch2; s != "buffer-write" {
		t.Errorf("conn2 read with WriteBuffer got %q", s)
	}
	msg1.Release()

	// Test ReadBuffer to select winner
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = srv2.Write([]byte("read-buffer"))
	}()
	readBuf := buf.New()
	defer readBuf.Release()
	if err = rc.ReadBuffer(readBuf); err != nil {
		t.Fatalf("ReadBuffer failed: %v", err)
	}
	if string(readBuf.Bytes()) != "read-buffer" {
		t.Fatalf("ReadBuffer got %q", readBuf.Bytes())
	}
	if rc.winner.proxy != p2 {
		t.Fatalf("winner was not p2 after ReadBuffer")
	}
}

func TestClose_BeforeWinner(t *testing.T) {
	mc1, _ := newMockConn()
	mc2, _ := newMockConn()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	if err = rcConn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	select {
	case <-mc1.closedCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("mc1 was not closed")
	}
	select {
	case <-mc2.closedCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("mc2 was not closed")
	}
}

func TestClose_AfterWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	// Select winner
	go func() { _, _ = srv2.Write([]byte("win")) }()
	_, err = rcConn.Read(make([]byte, 3))
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	<-mc1.closedCh // loser is closed by read

	// Close conn
	if err = rcConn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Winner should now be closed
	select {
	case <-mc2.closedCh:
		// expected
	case <-time.After(time.Second):
		t.Fatal("winner was not closed by Close()")
	}
}

func TestChains(t *testing.T) {
	mc1, _ := newMockConn()
	mc2, srv2 := newMockConn()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	chains := rcConn.Chains()
	if len(chains) != 0 {
		t.Fatalf("expected 0 chain, got %d", len(chains))
	}
	rcConn.AppendToChains(&mockProxy{name: "p3"})
	chains = rcConn.Chains()
	if len(chains) != 1 {
		t.Fatalf("expected 2 chains, got %d", len(chains))
	}

	go func() { _, _ = srv2.Write([]byte("win")) }()
	_, err = rcConn.Read(make([]byte, 3))
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	rcConn.AppendToChains(&mockProxy{name: "p4"})
	chains = rcConn.Chains()
	if len(chains) != 2 {
		t.Fatalf("expected 2 chains, got %d", len(chains))
	}
}

func TestReadDeadlineBeforeWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Set a short read deadline
	deadline := time.Now().Add(50 * time.Millisecond)
	if err := rcConn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("SetReadDeadline failed: %v", err)
	}

	// Read should time out as no data is written by servers
	buf := make([]byte, 1)
	_, err = rcConn.Read(buf)

	if err == nil {
		t.Fatal("read did not time out")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error, got: %v", err)
		}
	}
}

func TestReadDeadlineAfterWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Make p2 the winner
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = srv2.Write([]byte("win"))
	}()
	buf := make([]byte, 3)
	if _, err = rcConn.Read(buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	<-mc1.closedCh // Loser is closed

	// Set read deadline on winner
	deadline := time.Now().Add(50 * time.Millisecond)
	if err := rcConn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("SetReadDeadline failed: %v", err)
	}

	// Read should time out
	_, err = rcConn.Read(buf)
	if err == nil {
		t.Fatal("read did not time out after winner selected")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error, got: %v", err)
		}
	}
}

func TestWriteDeadlineBeforeWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	// Don't close servers immediately, to let Write block
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Set a short write deadline. This is broadcasted.
	deadline := time.Now().Add(100 * time.Millisecond)
	if err := rcConn.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline failed: %v", err)
	}

	// Write should time out as servers don't read
	// net.Pipe is unbuffered, so this will block and then timeout.
	msg := make([]byte, 1)
	_, err = rcConn.Write(msg)
	if err == nil {
		t.Fatal("write did not time out")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error, got: %v", err)
		}
	}
}

func TestWriteDeadlineAfterWinner(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	// Don't close srv2 immediately
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	// Make p2 the winner
	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = srv2.Write([]byte("win"))
	}()
	buf := make([]byte, 3)
	if _, err = rcConn.Read(buf); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Set write deadline on winner
	deadline := time.Now().Add(50 * time.Millisecond)
	if err := rcConn.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline failed: %v", err)
	}

	// Write should time out
	msg := make([]byte, 1)
	_, err = rcConn.Write(msg)
	if err == nil {
		t.Fatal("write did not time out")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error, got: %v", err)
		}
	}
}

func TestSetDeadline_Read(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	deadline := time.Now().Add(50 * time.Millisecond)
	if err := rcConn.SetDeadline(deadline); err != nil {
		t.Fatalf("SetDeadline failed: %v", err)
	}

	// Test read timeout
	buf := make([]byte, 1)
	_, err = rcConn.Read(buf)
	if err == nil {
		t.Fatal("read did not time out after SetDeadline")
	}

	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error for read, got: %v", err)
		}
	}
}

func TestSetDeadline_Write(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	deadline := time.Now().Add(100 * time.Millisecond)
	if err := rcConn.SetDeadline(deadline); err != nil {
		t.Fatalf("SetDeadline failed: %v", err)
	}

	// Test write timeout
	msg := make([]byte, 1)
	_, err = rcConn.Write(msg)
	if err == nil {
		t.Fatal("write did not time out after SetDeadline")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
			t.Fatalf("expected a timeout error for write, got: %v", err)
		}
	}
}

func TestMultipleWritesBeforeRead(t *testing.T) {
	mc1, srv1 := newMockConn()
	mc2, srv2 := newMockConn()
	defer srv1.Close()
	defer srv2.Close()
	p1 := &mockProxy{name: "p1", conn: mc1}
	p2 := &mockProxy{name: "p2", conn: mc2}

	rcConn, err := dialContext(context.Background(), nil, nil, p1, p2)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer rcConn.Close()

	msg1 := []byte("write1")
	msg2 := []byte("write2")
	totalLen := len(msg1) + len(msg2)

	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)

	go func() {
		buf := make([]byte, totalLen)
		// Use io.ReadFull to ensure all data is read
		if _, err := io.ReadFull(srv1, buf); err != nil {
			// t.Errorf is not safe to call from a different goroutine
			// We can close the channel and check later.
			close(ch1)
			return
		}
		ch1 <- string(buf)
	}()
	go func() {
		buf := make([]byte, totalLen)
		if _, err := io.ReadFull(srv2, buf); err != nil {
			close(ch2)
			return
		}
		ch2 <- string(buf)
	}()

	// Multiple writes before any read
	if _, err := rcConn.Write(msg1); err != nil {
		t.Fatalf("write1 failed: %v", err)
	}
	if _, err := rcConn.Write(msg2); err != nil {
		t.Fatalf("write2 failed: %v", err)
	}

	expectedMsg := string(msg1) + string(msg2)

	// Verify both connections received both writes
	select {
	case s, ok := <-ch1:
		if !ok {
			t.Fatal("srv1 read failed")
		}
		if s != expectedMsg {
			t.Errorf("conn1 got %q, want %q", s, expectedMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for conn1")
	}
	select {
	case s, ok := <-ch2:
		if !ok {
			t.Fatal("srv2 read failed")
		}
		if s != expectedMsg {
			t.Errorf("conn2 got %q, want %q", s, expectedMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for conn2")
	}

	// Now, select a winner to ensure the state transitions correctly.
	go func() { _, _ = srv2.Write([]byte("win")) }()
	readBuf := make([]byte, 3)
	if _, err := rcConn.Read(readBuf); err != nil {
		t.Fatalf("read to select winner failed: %v", err)
	}
	if string(readBuf) != "win" {
		t.Fatalf("read incorrect winner data: got %q", string(readBuf))
	}

	rc := rcConn.(*conn)
	if rc.winner.proxy != p2 {
		t.Fatalf("expected winner p2, got %s", rc.winner.proxy.Name())
	}
}

// --- Benchmarks ---

// benchProxy is a C.Proxy implementation for benchmarking.
// It embeds mockProxy and overrides dialContext to create an echo server.
type benchProxy struct {
	*mockProxy
}

func newBenchProxy(name string) *benchProxy {
	return &benchProxy{mockProxy: &mockProxy{name: name}}
}

func (p *benchProxy) DialContext(_ context.Context, _ *C.Metadata) (C.Conn, error) {
	mc, srv := newMockConn()
	go func() {
		defer srv.Close()
		// Simple echo server
		io.Copy(srv, srv)
	}()
	return mc, nil
}

func benchmarkRaceConnNRTT(b *testing.B, n int) {
	// Create a pool of 11 proxies for all benchmarks
	maxProxies := 11
	allProxies := make([]C.Proxy, maxProxies)
	for i := 0; i < maxProxies; i++ {
		allProxies[i] = newBenchProxy(fmt.Sprintf("bench-p%d", i))
	}

	proxyCounts := []int{1, 2, 3, 5, 7, 11}

	for _, count := range proxyCounts {
		proxies := allProxies[:count]
		b.Run(fmt.Sprintf("proxies-%d", count), func(b *testing.B) {
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
			OUT:
				for pb.Next() {
					rcConn, err := dialContext(context.Background(), nil, nil, proxies...)
					if err != nil {
						b.Errorf("dial failed: %v", err)
						continue
					}

					for i := 0; i < n; i++ {
						x := byte(rand.Intn(255))
						if _, err := rcConn.Write([]byte{x}); err != nil {
							b.Errorf("write failed: %v", err)
							rcConn.Close()
							continue OUT
						}

						readBuf := make([]byte, 1)
						if _, err := rcConn.Read(readBuf); err != nil {
							b.Errorf("read failed: %v", err)
							rcConn.Close()
							continue OUT
						}
						if readBuf[0] != x {
							b.Errorf("read failed: got %d, want %d", readBuf[0], x)
						}
					}

					rcConn.Close()
				}
			})
		})
	}
}

func BenchmarkRaceConn1RTT(b *testing.B) {
	benchmarkRaceConnNRTT(b, 1)
}

func BenchmarkRaceConn10RTT(b *testing.B) {
	benchmarkRaceConnNRTT(b, 10)
}
