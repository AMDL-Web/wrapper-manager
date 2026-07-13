package main

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDecryptServer struct {
	listener net.Listener
	accepts  atomic.Int32
	contexts atomic.Int32
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
}

func newFakeDecryptServer(t *testing.T) *fakeDecryptServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeDecryptServer{listener: listener, conns: make(map[net.Conn]struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
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
			if string(payload) == "fail" {
				return
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

	session, err := instance.OpenSession("song-1", "key-1")
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
	session, err = instance.OpenSession("song-1", "key-1")
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
	if got := server.contexts.Load(); got != 1 {
		t.Fatalf("context switches = %d, want 1", got)
	}
}

func TestDecryptSessionDoesNotReuseKeyAcrossSongs(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}

	for _, adamID := range []string{"song-1", "song-2"} {
		session, err := instance.OpenSession(adamID, "shared-key")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := session.Decrypt(adamID, "shared-key", []byte("sample")); err != nil {
			t.Fatal(err)
		}
		session.Close()
	}

	if got := server.contexts.Load(); got != 2 {
		t.Fatalf("context switches = %d, want 2", got)
	}
}

func TestDecryptFailureDiscardsOnlyFailedConnection(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}

	session, err := instance.OpenSession("song-1", "key-1")
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
	session, err = instance.OpenSession("song-1", "key-1")
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
	session, err := instance.OpenSession("song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
	session.Close()

	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if len(instance.pool) != 1 || instance.activeCount != 1 {
		t.Fatalf("pool=%d active=%d, want pool=1 active=1", len(instance.pool), instance.activeCount)
	}
}

func TestDecryptConnDeadlineIsCleared(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	session, err := instance.OpenSession("song-1", "key-1")
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
