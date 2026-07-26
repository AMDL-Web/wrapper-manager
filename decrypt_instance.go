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
	defaultId                = "0"
	prefetchKey              = "skd://itunes.apple.com/P000000000/s1/e1"
	decryptIOTimeout         = 30 * time.Second
	maxPoolSize              = 10
	wrapperFailureWindow     = 60 * time.Second
	wrapperFailureThreshold  = 3
	wrapperFailureMinConns   = 3
	wrapperFailureMinAdamIDs = 2
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

type wrapperIOFailure struct {
	at     time.Time
	conn   *decryptConn
	adamID string
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

	poolMu           sync.Mutex
	pool             []*decryptConn
	connections      map[*decryptConn]struct{}
	reserved         int
	isClosed         bool
	poolLimit        int
	dialContext      dialContextFunc
	ioTimeout        time.Duration
	onCapacity       func()
	onUnavailable    func(*DecryptInstance, string)
	terminateWrapper func() error
	now              func() time.Time

	healthMu sync.Mutex
	failures []wrapperIOFailure

	closeOnce       sync.Once
	unavailableOnce sync.Once
}

func NewDecryptInstance(inst *WrapperInstance) (*DecryptInstance, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	instance := &DecryptInstance{
		id:               inst.Id,
		region:           inst.Region,
		decryptPort:      inst.DecryptPort,
		pool:             make([]*decryptConn, 0, maxPoolSize),
		connections:      make(map[*decryptConn]struct{}, maxPoolSize),
		poolLimit:        maxPoolSize,
		dialContext:      dialer.DialContext,
		ioTimeout:        decryptIOTimeout,
		terminateWrapper: func() error { return terminateWrapperInstance(inst, wrapperTerminateGrace) },
		now:              time.Now,
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

func (d *DecryptInstance) Unavailable(reason string) {
	d.unavailableOnce.Do(func() {
		// Closing first immediately removes this instance from scheduling and
		// interrupts every leased connection. The wrapper lifecycle will replace
		// the process and register a fresh DecryptInstance after it exits.
		d.Close()
		logrus.Warnf("wrapper instance %s is unhealthy: %s; restarting", d.id, reason)
		if d.onUnavailable != nil {
			d.onUnavailable(d, reason)
		}
		if d.terminateWrapper == nil {
			logrus.Errorf("failed to restart instance %s: no wrapper kill function", d.id)
			return
		}
		// Process termination may wait for a grace period. It must not delay the
		// failed decrypt response or hold up healthy instances in the dispatcher.
		go func() {
			if err := d.terminateWrapper(); err != nil {
				logrus.Errorf("failed to terminate instance %s: %s", d.id, err)
			}
		}()
	})
}

func (s *DecryptSession) currentConn() (*decryptConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.conn == nil {
		return nil, errors.New("decrypt session is closed")
	}
	return s.conn, nil
}

// decryptFault marks a decrypt failure that a different wrapper instance could
// plausibly satisfy: the local wrapper misbehaved (as opposed to the client
// going away), so replaying the same sample elsewhere is worth trying.
//
// replayable additionally records that the request sample is still byte-for-byte
// as the client sent it. Decryption reads the plaintext back over the request
// buffer, so a read that already delivered some bytes has overwritten part of
// the ciphertext; replaying that would hand the next instance a corrupt sample
// and yield silently wrong audio rather than an error.
type decryptFault struct {
	instance   *DecryptInstance
	err        error
	replayable bool
}

func (f *decryptFault) Error() string { return f.err.Error() }

func (f *decryptFault) Unwrap() error { return f.err }

// fault wraps err for the failover path. Non-local errors (client cancellation,
// client deadline) are returned unwrapped: retrying them elsewhere would only
// burn another instance's capacity on work nobody is waiting for.
func (s *DecryptSession) fault(err error, local, replayable bool) error {
	if !local {
		return err
	}
	return &decryptFault{instance: s.instance, err: err, replayable: replayable}
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
			s.instance.observeWrapperIOFailure(s.ctx, c, adamId, "context switch", err)
			local, _ := classifyLocalWrapperIOError(s.ctx, err)
			s.Discard()
			// The sample was never written to the wrapper, so it is always
			// intact here regardless of how the context switch failed.
			return nil, s.fault(mapContextError(s.ctx, err), local, true)
		}
	}
	// Recv gives each request its own sample storage. Once the encrypted bytes
	// are written to the wrapper, read the plaintext back into that same slice.
	// The slice is sent once and is never modified by a later request.
	result, read, err := s.instance.decryptConn(s.ctx, c, payload, payload)
	if err != nil {
		s.instance.observeWrapperIOFailure(s.ctx, c, adamId, "decrypt", err)
		local, _ := classifyLocalWrapperIOError(s.ctx, err)
		s.Discard()
		return nil, s.fault(mapContextError(s.ctx, err), local, read == 0)
	}
	return result, nil
}

func classifyLocalWrapperIOError(ctx context.Context, err error) (local, timedOut bool) {
	if err == nil {
		return false, false
	}
	// A client cancellation or client-owned deadline is not evidence that the
	// local wrapper process is unhealthy.
	if ctx != nil && ctx.Err() != nil {
		return false, false
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return false, false
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true, false
	}
	var netErr net.Error
	if !errors.As(err, &netErr) {
		return false, false
	}
	return true, netErr.Timeout()
}

func (d *DecryptInstance) observeWrapperIOFailure(ctx context.Context, conn *decryptConn, adamID, stage string, err error) {
	local, timedOut := classifyLocalWrapperIOError(ctx, err)
	if conn == nil || adamID == "" || !local {
		return
	}
	d.poolMu.Lock()
	closed := d.isClosed
	d.poolMu.Unlock()
	if closed {
		return
	}
	// The deadline owned by this manager is thirty seconds. A loopback wrapper
	// operation exceeding it is already conclusive evidence of a wedged local
	// process; waiting for more timeouts only prolongs the outage, especially at
	// concurrency one.
	if timedOut {
		d.Unavailable(fmt.Sprintf("local %s I/O timed out for Adam ID %s", stage, adamID))
		return
	}

	now := time.Now()
	if d.now != nil {
		now = d.now()
	}
	cutoff := now.Add(-wrapperFailureWindow)

	d.healthMu.Lock()
	kept := d.failures[:0]
	for _, failure := range d.failures {
		if !failure.at.Before(cutoff) {
			kept = append(kept, failure)
		}
	}
	d.failures = append(kept, wrapperIOFailure{
		at:     now,
		conn:   conn,
		adamID: adamID,
	})

	connections := make(map[*decryptConn]struct{}, len(d.failures))
	adamIDs := make(map[string]struct{}, len(d.failures))
	for _, failure := range d.failures {
		connections[failure.conn] = struct{}{}
		adamIDs[failure.adamID] = struct{}{}
	}
	failureCount := len(d.failures)
	shouldTrip := failureCount >= wrapperFailureThreshold && len(connections) >= wrapperFailureMinConns && len(adamIDs) >= wrapperFailureMinAdamIDs
	d.healthMu.Unlock()

	if !shouldTrip {
		logrus.Warnf("wrapper instance %s local %s I/O failure (%d/%d in %s): %v", d.id, stage, failureCount, wrapperFailureThreshold, wrapperFailureWindow, err)
		return
	}
	d.Unavailable(fmt.Sprintf(
		"%d local I/O failures across %d connections and %d Adam IDs in %s",
		failureCount, len(connections), len(adamIDs), wrapperFailureWindow,
	))
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

// decryptConn returns the plaintext along with the number of bytes read into
// the plaintext buffer. Callers that alias plaintext onto the request sample
// need that count to know whether the request survived a failure intact: only
// a zero-byte read leaves the ciphertext replayable on another instance.
func (d *DecryptInstance) decryptConn(ctx context.Context, c *decryptConn, sample, plaintext []byte) ([]byte, int, error) {
	if len(sample) == 0 {
		return nil, 0, errors.New("empty decrypt sample")
	}
	if len(plaintext) != len(sample) {
		return nil, 0, errors.New("plaintext buffer length does not match decrypt sample")
	}
	if err := d.setOperationDeadline(ctx, c.conn); err != nil {
		return nil, 0, err
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
		return nil, 0, writeErr
	}
	read, err := io.ReadFull(c.conn, plaintext)
	if err != nil {
		return nil, read, err
	}
	return plaintext, read, nil
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
