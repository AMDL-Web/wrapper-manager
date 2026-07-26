package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDecryptServer struct {
	listener   net.Listener
	acceptDone chan struct{}
	accepts    atomic.Int32
	contexts   atomic.Int32
	samples    atomic.Int32
	// faulty makes the server drop the connection after reading a sample,
	// whatever the payload, so one instance can be sick while another stays
	// healthy on the same request bytes.
	faulty atomic.Bool
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
}

type fakeTimeoutError struct{}

func (fakeTimeoutError) Error() string   { return "i/o timeout" }
func (fakeTimeoutError) Timeout() bool   { return true }
func (fakeTimeoutError) Temporary() bool { return true }

func newFakeDecryptServer(t *testing.T) *fakeDecryptServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeDecryptServer{listener: listener, acceptDone: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			s.accepts.Add(1)
			s.mu.Lock()
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() {
					_ = conn.Close()
					s.mu.Lock()
					delete(s.conns, conn)
					s.mu.Unlock()
				}()
				s.serveConn(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		// Ensure Accept cannot publish another connection after the cleanup
		// snapshot. WaitGroup.Add and Wait are also no longer concurrent.
		<-s.acceptDone
		s.mu.Lock()
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *fakeDecryptServer) port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *fakeDecryptServer) serveConn(conn net.Conn) {
	for {
		var adamLength uint8
		if binary.Read(conn, binary.LittleEndian, &adamLength) != nil {
			return
		}
		adam := make([]byte, adamLength)
		if _, err := io.ReadFull(conn, adam); err != nil {
			return
		}
		var keyLength uint8
		if binary.Read(conn, binary.LittleEndian, &keyLength) != nil {
			return
		}
		key := make([]byte, keyLength)
		if _, err := io.ReadFull(conn, key); err != nil {
			return
		}
		s.contexts.Add(1)

		for {
			var size uint32
			if binary.Read(conn, binary.LittleEndian, &size) != nil {
				return
			}
			if size == 0 {
				break
			}
			payload := make([]byte, size)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
			s.samples.Add(1)
			if s.faulty.Load() {
				return
			}
			if string(payload) == "fail" {
				return
			}
			if string(payload) == "block" {
				continue
			}
			if string(payload) == "partial" {
				partial := append([]byte(nil), payload[:len(payload)/2]...)
				for i := range partial {
					partial[i] = 0xAA
				}
				_, _ = conn.Write(partial)
				return
			}
			if string(payload) == "transform" {
				for i := range payload {
					payload[i] ^= 0xFF
				}
			}
			if _, err := conn.Write(payload); err != nil {
				return
			}
		}
	}
}

func TestDecryptSessionKeepsConnectionAcrossSamples(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte("one"), []byte("two"), []byte("three")} {
		got, err := session.Decrypt("song-1", "key-1", payload)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(payload) {
			t.Fatalf("got %q, want %q", got, payload)
		}
	}
	session.Close()

	// The next stream for the same song/key should reuse the idle connection
	// and its existing wrapper context.
	session, err = instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("four")); err != nil {
		t.Fatal(err)
	}
	session.Close()

	if got := server.accepts.Load(); got != 1 {
		t.Fatalf("accepted connections = %d, want 1", got)
	}
	if got := server.contexts.Load(); got != 2 {
		t.Fatalf("context switches = %d, want 2 (pre-warm plus song)", got)
	}
}

func TestDecryptSessionDoesNotReuseKeyAcrossSongs(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}

	for _, adamID := range []string{"song-1", "song-2"} {
		session, err := instance.OpenSession(context.Background(), adamID, "shared-key")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.Decrypt(adamID, "shared-key", []byte("sample")); err != nil {
			t.Fatal(err)
		}
		session.Close()
	}

	if got := server.contexts.Load(); got != 3 {
		t.Fatalf("context switches = %d, want 3 (pre-warm plus two songs)", got)
	}
}

func TestDecryptFailureDiscardsOnlyFailedConnection(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("fail")); err == nil {
		t.Fatal("expected decrypt failure")
	}
	if instance.isClosed {
		t.Fatal("one connection failure closed the whole wrapper instance")
	}

	// The same instance must remain usable through a replacement connection.
	session, err = instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Decrypt("song-1", "key-1", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if got := server.accepts.Load(); got != 2 {
		t.Fatalf("accepted connections = %d, want 2", got)
	}
}

func TestDecryptSessionCloseIsIdempotent(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
	session.Close()

	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if len(instance.pool) != 1 || len(instance.connections) != 1 {
		t.Fatalf("pool=%d connections=%d, want pool=1 connections=1", len(instance.pool), len(instance.connections))
	}
}

func TestCanceledOpenSessionReturnsIdleConnection(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := instance.OpenSession(ctx, "song-1", "key-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenSession error = %v, want context canceled", err)
	}

	instance.poolMu.Lock()
	poolSize := len(instance.pool)
	connectionCount := len(instance.connections)
	instance.poolMu.Unlock()
	if poolSize != 1 || connectionCount != 1 {
		t.Fatalf("pool=%d connections=%d, want idle connection returned", poolSize, connectionCount)
	}

	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
}

func TestCancellationAfterDialDoesNotLeakConnectionAccounting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	instance := &DecryptInstance{
		connections: make(map[*decryptConn]struct{}),
		poolLimit:   1,
		ioTimeout:   time.Second,
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			_ = server.Close()
			cancel()
			return client, nil
		},
	}
	if _, err := instance.OpenSession(ctx, "song-1", "key-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenSession error = %v, want context canceled", err)
	}

	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if instance.reserved != 0 || len(instance.connections) != 0 {
		t.Fatalf("canceled dial leaked accounting: reserved=%d connections=%d", instance.reserved, len(instance.connections))
	}
}

func TestDecryptConnDeadlineIsCleared(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Decrypt("song-1", "key-1", []byte("sample")); err != nil {
		t.Fatal(err)
	}
	// Waiting beyond a tiny interval catches accidental short test deadlines;
	// production uses five seconds, so this mostly documents reusable sessions.
	time.Sleep(time.Millisecond)
	if _, err := session.Decrypt("song-1", "key-1", []byte("sample")); err != nil {
		t.Fatal(err)
	}
}

func TestDecryptCancellationInterruptsBlockedWrapperIO(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	session, err := instance.OpenSession(ctx, "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	_, err = session.Decrypt("song-1", "key-1", []byte("block"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Decrypt error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked I/O cancellation took %s", elapsed)
	}
}

func TestDecryptCancellationWithoutDeadlineInterruptsBlockedWrapperIO(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	session, err := instance.OpenSession(ctx, "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := session.Decrypt("song-1", "key-1", []byte("block"))
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Decrypt error = %v, want context canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("blocked I/O did not stop after context cancellation")
	}
}

func TestWrapperIOTimeoutQuarantinesAndTerminatesInstanceOnce(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	instance.ioTimeout = 30 * time.Millisecond
	terminated := make(chan struct{}, 2)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}

	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("block")); err == nil {
		t.Fatal("expected wrapper I/O timeout")
	}
	select {
	case <-terminated:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wrapper termination")
	}
	instance.Unavailable("duplicate trigger")
	select {
	case <-terminated:
		t.Fatal("wrapper termination ran more than once")
	case <-time.After(50 * time.Millisecond):
	}
	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if !instance.isClosed || len(instance.connections) != 0 || len(instance.pool) != 0 {
		t.Fatalf("unhealthy instance was not quarantined: closed=%v connections=%d pool=%d", instance.isClosed, len(instance.connections), len(instance.pool))
	}
}

func TestConcurrentWrapperIOTimeoutsScheduleSingleTermination(t *testing.T) {
	instance := &DecryptInstance{
		id:          "test",
		connections: make(map[*decryptConn]struct{}),
		poolLimit:   maxPoolSize,
	}
	var terminations atomic.Int32
	terminationStarted := make(chan struct{})
	releaseTermination := make(chan struct{})
	instance.terminateWrapper = func() error {
		if terminations.Add(1) == 1 {
			close(terminationStarted)
		}
		<-releaseTermination
		return nil
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < maxPoolSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			instance.observeWrapperIOFailure(
				context.Background(),
				&decryptConn{},
				fmt.Sprintf("song-%d", i),
				"decrypt",
				fakeTimeoutError{},
			)
		}(i)
	}
	close(start)
	returned := make(chan struct{})
	go func() {
		wg.Wait()
		close(returned)
	}()
	select {
	case <-terminationStarted:
	case <-time.After(time.Second):
		t.Fatal("wrapper termination did not start")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("failure reporters blocked on wrapper termination")
	}
	if got := terminations.Load(); got != 1 {
		t.Fatalf("wrapper terminations = %d, want 1", got)
	}
	close(releaseTermination)
}

func TestClientDeadlineDoesNotQuarantineWrapper(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	instance.ioTimeout = time.Second
	var terminations atomic.Int32
	instance.terminateWrapper = func() error {
		terminations.Add(1)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	session, err := instance.OpenSession(ctx, "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("block")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Decrypt error = %v, want deadline exceeded", err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := terminations.Load(); got != 0 {
		t.Fatalf("client deadline triggered %d wrapper terminations", got)
	}
	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if instance.isClosed {
		t.Fatal("client deadline quarantined wrapper instance")
	}
}

func TestRepeatedConnectionFailuresAcrossSongsQuarantineOnce(t *testing.T) {
	instance := &DecryptInstance{
		id:          "test",
		connections: make(map[*decryptConn]struct{}),
		poolLimit:   maxPoolSize,
		now:         time.Now,
	}
	terminated := make(chan struct{}, 2)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}
	for i, adamID := range []string{"song-1", "song-1", "song-2"} {
		instance.observeWrapperIOFailure(context.Background(), &decryptConn{}, adamID, "decrypt", io.EOF)
		if i < 2 {
			select {
			case <-terminated:
				t.Fatalf("wrapper terminated after only %d connection failures", i+1)
			default:
			}
		}
	}
	select {
	case <-terminated:
	case <-time.After(time.Second):
		t.Fatal("repeated cross-song connection failures did not terminate wrapper")
	}
	instance.Unavailable("duplicate trigger")
	select {
	case <-terminated:
		t.Fatal("wrapper termination ran more than once")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDecryptPartialFailureDiscardsConnection(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("partial")
	if _, err := session.Decrypt("song-1", "key-1", payload); err == nil {
		t.Fatal("expected partial response failure")
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("sample")); err == nil {
		t.Fatal("failed session remained usable")
	}
}

func TestDecryptSuccessUsesPerRequestStorage(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	payload := []byte("transform")
	result, err := session.Decrypt("song-1", "key-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if &result[0] != &payload[0] {
		t.Fatal("decrypt did not reuse the request's sample storage")
	}
	if string(result) == "transform" {
		t.Fatal("fake wrapper did not return transformed plaintext")
	}
	firstResult := result
	wantFirst := append([]byte(nil), result...)

	payload = []byte("sample")
	secondResult, err := session.Decrypt("song-1", "key-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondResult) == 0 {
		t.Fatal("second decrypt returned no plaintext")
	}
	if string(firstResult) != string(wantFirst) {
		t.Fatalf("first reply changed after the next decrypt: %x", firstResult)
	}
}
