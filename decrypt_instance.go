package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultId        = "0"
	prefetchKey      = "skd://itunes.apple.com/P000000000/s1/e1"
	decryptIOTimeout = 30 * time.Second
	maxPoolSize      = 10
)

var errInstanceBusy = errors.New("decrypt instance is at capacity")

type dialContextFunc func(context.Context, string, string) (net.Conn, error)

type decryptConn struct {
	conn         net.Conn
	lastAdamId   string
	lastKey      string
	writeHeader  [4]byte
	writeParts   [2][]byte
	writeBuffers net.Buffers
}

// DecryptSession leases one wrapper connection for the lifetime of a client
// gRPC stream. The stream context cancels pool waits, dials, and blocked I/O.
type DecryptSession struct {
	instance   *DecryptInstance
	conn       *decryptConn
	ctx        context.Context
	stopCancel func() bool

	mu     sync.Mutex
	closed bool
}

type instanceLoad struct {
	inUse       int
	hasCapacity bool
	contextHit  bool
}

type DecryptInstance struct {
	id          string
	region      string
	decryptPort int

	poolMu      sync.Mutex
	pool        []*decryptConn
	connections map[*decryptConn]struct{}
	reserved    int
	isClosed    bool
	poolLimit   int
	dialContext dialContextFunc
	ioTimeout   time.Duration
	onCapacity  func()

	closeOnce sync.Once
}

func NewDecryptInstance(inst *WrapperInstance) (*DecryptInstance, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	instance := &DecryptInstance{
		id:          inst.Id,
		region:      inst.Region,
		decryptPort: inst.DecryptPort,
		pool:        make([]*decryptConn, 0, maxPoolSize),
		connections: make(map[*decryptConn]struct{}, maxPoolSize),
		poolLimit:   maxPoolSize,
		dialContext: dialer.DialContext,
		ioTimeout:   decryptIOTimeout,
	}

	// Pre-warm one connection both to validate the wrapper and to keep the
	// preshare context ready. Construction fails atomically if the dial fails.
	reserved, needsDial, ok := instance.reserveConn(defaultId, prefetchKey)
	if !ok {
		return nil, errors.New("failed to reserve wrapper pre-warm connection")
	}
	session, err := instance.openReserved(context.Background(), reserved, needsDial)
	if err != nil {
		return nil, err
	}
	if err := instance.switchConnContext(context.Background(), session.conn, defaultId, prefetchKey); err != nil {
		session.Discard()
		return nil, err
	}
	session.Close()
	return instance, nil
}

// snapshotLoad is advisory; reserveConn is the authoritative capacity check.
func (d *DecryptInstance) snapshotLoad(adamId, key string) instanceLoad {
	d.poolMu.Lock()
	defer d.poolMu.Unlock()

	load := instanceLoad{inUse: len(d.connections) - len(d.pool) + d.reserved}
	if d.isClosed {
		return load
	}
	load.hasCapacity = len(d.pool) > 0 || len(d.connections)+d.reserved < d.poolLimit
	for _, c := range d.pool {
		if c.lastAdamId == adamId && c.lastKey == key {
			load.contextHit = true
			break
		}
	}
	return load
}

// reserveConn atomically checks capacity and leases an idle connection or a
// slot for a new dial. It never blocks and never performs network I/O.
func (d *DecryptInstance) reserveConn(adamId, key string) (*decryptConn, bool, bool) {
	d.poolMu.Lock()
	defer d.poolMu.Unlock()
	if d.isClosed {
		return nil, false, false
	}
	for i, c := range d.pool {
		if c.lastAdamId == adamId && c.lastKey == key {
			d.pool = append(d.pool[:i], d.pool[i+1:]...)
			return c, false, true
		}
	}
	if n := len(d.pool); n > 0 {
		c := d.pool[n-1]
		d.pool = d.pool[:n-1]
		return c, false, true
	}
	if len(d.connections)+d.reserved >= d.poolLimit {
		return nil, false, false
	}
	d.reserved++
	return nil, true, true
}

func (d *DecryptInstance) openReserved(ctx context.Context, c *decryptConn, needsDial bool) (*DecryptSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		if needsDial {
			d.poolMu.Lock()
			d.reserved--
			d.poolMu.Unlock()
			d.signalCapacity()
		} else {
			d.releaseConn(c)
		}
		return nil, err
	}
	if needsDial {
		rawConn, err := d.dialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", d.decryptPort))
		d.poolMu.Lock()
		d.reserved--
		closed := d.isClosed
		ctxErr := ctx.Err()
		if err == nil && !closed && ctxErr == nil {
			c = &decryptConn{conn: rawConn}
			d.connections[c] = struct{}{}
		}
		d.poolMu.Unlock()
		if err != nil {
			if rawConn != nil {
				_ = rawConn.Close()
			}
			d.signalCapacity()
			return nil, err
		}
		if closed || ctxErr != nil {
			_ = rawConn.Close()
			d.signalCapacity()
			if ctxErr != nil {
				return nil, ctxErr
			}
			return nil, errors.New("decrypt instance is closed")
		}
	}

	s := &DecryptSession{instance: d, conn: c, ctx: ctx}
	s.stopCancel = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.closed && s.conn != nil {
			_ = s.conn.conn.SetDeadline(time.Now())
		}
	})
	if err := ctx.Err(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// OpenSession is a non-blocking instance-level acquisition. Dispatcher is
// responsible for waiting across all instances when every pool is full.
func (d *DecryptInstance) OpenSession(ctx context.Context, adamId, key string) (*DecryptSession, error) {
	c, needsDial, ok := d.reserveConn(adamId, key)
	if !ok {
		return nil, errInstanceBusy
	}
	return d.openReserved(ctx, c, needsDial)
}

func (d *DecryptInstance) releaseConn(c *decryptConn) {
	if c == nil {
		return
	}
	closeConn := false
	d.poolMu.Lock()
	if d.isClosed {
		closeConn = true
	} else if _, ok := d.connections[c]; ok {
		d.pool = append(d.pool, c)
	} else {
		closeConn = true
	}
	d.poolMu.Unlock()
	if closeConn {
		_ = c.conn.Close()
	}
	d.signalCapacity()
}

func (d *DecryptInstance) discardConn(c *decryptConn) {
	if c == nil {
		return
	}
	d.poolMu.Lock()
	delete(d.connections, c)
	d.poolMu.Unlock()
	_ = c.conn.Close()
	d.signalCapacity()
}

func (d *DecryptInstance) signalCapacity() {
	if d.onCapacity != nil {
		d.onCapacity()
	}
}

// Close terminates every idle or leased connection without changing wrapper
// process state. Session cleanup after Close is idempotent and cannot underflow
// connection accounting.
func (d *DecryptInstance) Close() {
	d.closeOnce.Do(func() {
		d.poolMu.Lock()
		d.isClosed = true
		connections := make([]*decryptConn, 0, len(d.connections))
		for c := range d.connections {
			connections = append(connections, c)
		}
		d.connections = make(map[*decryptConn]struct{})
		d.pool = nil
		d.poolMu.Unlock()
		for _, c := range connections {
			_ = c.conn.Close()
		}
		d.signalCapacity()
	})
}

func (d *DecryptInstance) Unavailable() {
	d.Close()
	if err := KillWrapper(d.id); err != nil {
		logrus.Errorf("failed to kill instance %s: %s", d.id, err)
	}
}

func (s *DecryptSession) currentConn() (*decryptConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return nil, errors.New("decrypt session is closed")
	}
	return s.conn, nil
}

func (s *DecryptSession) Decrypt(adamId, key string, payload []byte) ([]byte, error) {
	if err := s.ctx.Err(); err != nil {
		s.Discard()
		return nil, err
	}
	c, err := s.currentConn()
	if err != nil {
		return nil, err
	}
	if c.lastAdamId != adamId || c.lastKey != key {
		if err := s.instance.switchConnContext(s.ctx, c, adamId, key); err != nil {
			s.Discard()
			return nil, mapContextError(s.ctx, err)
		}
	}
	// Recv gives each request its own sample storage. Once the encrypted bytes
	// are written to the wrapper, read the plaintext back into that same slice.
	// The slice is sent once and is never modified by a later request.
	result, err := s.instance.decryptConn(s.ctx, c, payload, payload)
	if err != nil {
		s.Discard()
		return nil, mapContextError(s.ctx, err)
	}
	return result, nil
}

func mapContextError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	var netErr net.Error
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) && errors.As(err, &netErr) && netErr.Timeout() {
		return context.DeadlineExceeded
	}
	return err
}

func (s *DecryptSession) takeConn() (*decryptConn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false
	}
	s.closed = true
	c := s.conn
	s.conn = nil
	if s.stopCancel != nil {
		s.stopCancel()
	}
	return c, true
}

func (s *DecryptSession) Close() {
	c, ok := s.takeConn()
	if !ok {
		return
	}
	_ = c.conn.SetDeadline(time.Time{})
	s.instance.releaseConn(c)
}

func (s *DecryptSession) Discard() {
	c, ok := s.takeConn()
	if !ok {
		return
	}
	s.instance.discardConn(c)
}

func (d *DecryptInstance) setOperationDeadline(ctx context.Context, conn net.Conn) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(d.ioTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	// Cancellation can race the future deadline above. Recheck after setting it
	// so an already-fired cancellation can never be extended to ioTimeout.
	if err := ctx.Err(); err != nil {
		_ = conn.SetDeadline(time.Now())
		return err
	}
	return nil
}

func (d *DecryptInstance) decryptConn(ctx context.Context, c *decryptConn, sample, plaintext []byte) ([]byte, error) {
	if len(sample) == 0 {
		return nil, errors.New("empty decrypt sample")
	}
	if len(plaintext) != len(sample) {
		return nil, errors.New("plaintext buffer length does not match decrypt sample")
	}
	if err := d.setOperationDeadline(ctx, c.conn); err != nil {
		return nil, err
	}
	binary.LittleEndian.PutUint32(c.writeHeader[:], uint32(len(sample)))
	c.writeParts[0] = c.writeHeader[:]
	c.writeParts[1] = sample
	c.writeBuffers = c.writeParts[:]
	_, writeErr := c.writeBuffers.WriteTo(c.conn)
	// Do not retain the request sample for the lifetime of the pooled connection.
	c.writeParts[0] = nil
	c.writeParts[1] = nil
	c.writeBuffers = nil
	if writeErr != nil {
		return nil, writeErr
	}
	if _, err := io.ReadFull(c.conn, plaintext); err != nil {
		return nil, err
	}
	return plaintext, nil
}

func (d *DecryptInstance) switchConnContext(ctx context.Context, c *decryptConn, adamId, key string) error {
	if err := d.setOperationDeadline(ctx, c.conn); err != nil {
		return err
	}
	if c.lastKey != "" {
		if _, err := c.conn.Write([]byte{0, 0, 0, 0}); err != nil {
			return err
		}
	}
	id := adamId
	if key == prefetchKey {
		id = defaultId
	}
	if len(id) > 255 || len(key) > 255 {
		return errors.New("wrapper context identifier is too long")
	}
	var header [2]byte
	header[0] = byte(len(id))
	header[1] = byte(len(key))
	parts := net.Buffers{header[:1], []byte(id), header[1:], []byte(key)}
	if _, err := parts.WriteTo(c.conn); err != nil {
		return err
	}
	c.lastAdamId = adamId
	c.lastKey = key
	return nil
}
