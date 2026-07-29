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
	defaultId   = "0"
	prefetchKey = "skd://itunes.apple.com/P000000000/s1/e1"
	// defaultDecryptIOTimeout bounds one wrapper request/reply over loopback:
	// a single sample, or one context switch. It is not a per-fragment or
	// per-track budget.
	//
	// Sizing it from a blended mean gives the wrong answer, and an earlier
	// single value of three seconds came from exactly that mistake. Splitting
	// the measurement by population shows why. Over two 8-track hi-res ALAC
	// albums on rog:
	//
	//   context-switch write    25-131 µs         never a factor
	//   steady-state decrypt    p99 <= 2 ms, worst 4.751 ms over ~51,000
	//   first decrypt after a   743 ms - 2.259 s on a healthy instance
	//   context switch          (the wrapper's key setup lands on this read)
	//
	// So the tail is not random jitter spread through the run: it is one or two
	// operations per track, exactly where a key changes, and it is four orders
	// of magnitude away from the samples around it. One deadline covering both
	// populations has to be sized for the slow one, which leaves the 99.97% of
	// operations that are steady-state waiting far longer than they ever need
	// to before a wedged wrapper is noticed.
	//
	// Two seconds is ~400x the worst steady-state sample observed.
	defaultDecryptIOTimeout = 2 * time.Second
	// defaultFirstSampleIOTimeout covers the first decrypt after a context
	// switch. Ten seconds looked like 4.4x headroom over the 2.259 s worst case
	// in the first two albums; production then produced 8.327 s completions and
	// operations sitting on the 10 s deadline itself, so that run had simply not
	// seen the tail. This population has no characterised ceiling.
	//
	// Thirty seconds is where this deadline started, and in that form it never
	// misfired. Since a timeout here no longer counts against instance health,
	// the only thing a budget this wide costs is how long one operation per track
	// waits before failing over — and cutting it is what turned legitimate slow
	// operations into restarts of healthy wrappers.
	defaultFirstSampleIOTimeout = 30 * time.Second
	maxPoolSize                 = 10
	wrapperFailureWindow        = 60 * time.Second
	wrapperFailureThreshold     = 3
	wrapperFailureMinConns      = 3
	wrapperFailureMinAdamIDs    = 2
	// wrapperTimeoutThreshold is how many timeouts inside wrapperFailureWindow
	// declare the local wrapper wedged. A timeout no longer condemns the process
	// on its own: at defaultDecryptIOTimeout a single one is a host hiccup as
	// plausibly as a dead wrapper, and killing a healthy instance costs every
	// session it holds. Unlike wrapperFailureThreshold this rule asks nothing of
	// connection or Adam ID diversity, because a wedged wrapper starves its
	// caller instead of producing varied traffic — at concurrency one there is
	// only ever one connection and one song to observe.
	//
	// Two timeouts still confirm the fault sooner than the single thirty-second
	// one this replaces, and decryptWithFailover has already rescued the samples
	// spent getting there.
	wrapperTimeoutThreshold = 2
	// firstSampleStage names the operation whose timeouts are routed only to
	// the streak rule; observeWrapperIOFailure compares against it.
	firstSampleStage = "first decrypt after context switch"
)

// emptyPoolGrace is how long a decrypt waits for an instance when every one of
// them is restarting, before giving up. A replacement took 72s on 2026-07-29.
// Dispatcher.canCondemn keeps the pool from emptying while a replacement may
// still be arriving, so this covers the cases it deliberately allows through: a
// total loss, a sole instance being restarted, and the bounded case where a
// replacement never came and the wedged instance holding the pool open was
// condemned anyway. In all of them waiting out a restart still beats failing,
// and a manager with no wrappers configured at all must still answer rather
// than hang.
const emptyPoolGrace = 90 * time.Second

// The effective deadlines, overridable at startup by -decrypt-timeout and
// -first-sample-timeout for hosts slower than the benchmarked ones.
var (
	decryptIOTimeout     = defaultDecryptIOTimeout
	firstSampleIOTimeout = defaultFirstSampleIOTimeout
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
	adamID     string
	// Three populations, kept apart because they are not interchangeable when
	// sizing the deadline: the context-switch write itself, the first decrypt
	// after one (where the wrapper's key setup is expected to land), and every
	// other decrypt.
	switchLatency sampleLatency
	firstLatency  sampleLatency
	latency       sampleLatency

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

	poolMu             sync.Mutex
	pool               []*decryptConn
	connections        map[*decryptConn]struct{}
	reserved           int
	isClosed           bool
	poolLimit          int
	dialContext        dialContextFunc
	ioTimeout          time.Duration
	firstSampleTimeout time.Duration
	onCapacity         func()
	onUnavailable      func(*DecryptInstance, string)
	// canCondemn reports whether this instance may be taken out of service now.
	// Nil means unconditionally; see Dispatcher.canCondemn for why it is not.
	canCondemn       func() bool
	terminateWrapper func() error
	now              func() time.Time

	healthMu sync.Mutex
	failures []wrapperIOFailure
	timeouts []time.Time
	// firstSampleTimeouts is deliberately separate from timeouts. The two
	// count different populations under the same threshold: a steady-state
	// timeout is evidence that survives its window, while a first-sample one
	// is only evidence as an unbroken streak, and is dropped the moment any
	// first sample completes. Sharing one slice let a completed first sample
	// erase steady-state evidence, which un-quarantined a genuinely wedged
	// wrapper on every context switch.
	firstSampleTimeouts []time.Time

	closeOnce       sync.Once
	unavailableOnce sync.Once
}

func NewDecryptInstance(inst *WrapperInstance) (*DecryptInstance, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	instance := &DecryptInstance{
		id:                 inst.Id,
		region:             inst.Region,
		decryptPort:        inst.DecryptPort,
		pool:               make([]*decryptConn, 0, maxPoolSize),
		connections:        make(map[*decryptConn]struct{}, maxPoolSize),
		poolLimit:          maxPoolSize,
		dialContext:        dialer.DialContext,
		ioTimeout:          decryptIOTimeout,
		firstSampleTimeout: firstSampleIOTimeout,
		terminateWrapper:   func() error { return terminateWrapperInstance(inst, wrapperTerminateGrace) },
		now:                time.Now,
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
	// Deliberately outside unavailableOnce: a declined condemnation must not
	// consume the one shot, or the instance could never be condemned once a
	// replacement has arrived.
	if d.canCondemn != nil && !d.canCondemn() {
		logrus.Warnf("wrapper instance %s is unhealthy (%s) but is the last one serving while a replacement started less than %s ago; keeping it until the pool refills, that replacement is declared failed, or the grace expires", d.id, reason, pendingReplacementGrace)
		return
	}
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
	switched := false
	if c.lastAdamId != adamId || c.lastKey != key {
		switchStarted := time.Now()
		switchErr := s.instance.switchConnContext(s.ctx, c, adamId, key)
		s.switchLatency.observe(time.Since(switchStarted))
		if switchErr != nil {
			// The switch is a pure write, measured at 25-131 µs, so its budget is
			// four orders of magnitude clear of the distribution: a timeout here
			// really does mean the wrapper stopped reading.
			s.instance.observeWrapperIOFailure(s.ctx, c, adamId, "context switch", true, switchErr)
			local, _ := classifyLocalWrapperIOError(s.ctx, switchErr)
			s.Discard()
			// The sample was never written to the wrapper, so it is always
			// intact here regardless of how the context switch failed.
			return nil, s.fault(mapContextError(s.ctx, switchErr), local, true)
		}
		switched = true
	}
	// Recv gives each request its own sample storage. Once the encrypted bytes
	// are written to the wrapper, read the plaintext back into that same slice.
	// The slice is sent once and is never modified by a later request.
	s.adamID = adamId
	budget := s.instance.ioTimeout
	if switched {
		budget = s.instance.firstSampleTimeout
	}
	started := time.Now()
	result, read, err := s.instance.decryptConn(s.ctx, c, payload, payload, budget)
	elapsed := time.Since(started)
	if switched {
		s.firstLatency.observe(elapsed)
		if err == nil {
			// This instance can still complete a key setup, however slowly, so
			// whatever timeouts preceded this were slowness and not a wedge.
			s.instance.resetFirstSampleFailures()
		}
	} else {
		s.latency.observe(elapsed)
	}
	if err != nil {
		stage, conclusive := "decrypt", true
		if switched {
			// A first decrypt after a context switch is slow by nature, so
			// merely exceeding the budget says nothing — except when the
			// wrapper produced no bytes at all. See observeWrapperIOFailure.
			stage, conclusive = firstSampleStage, read == 0
		}
		s.instance.observeWrapperIOFailure(s.ctx, c, adamId, stage, conclusive, err)
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

// resetFirstSampleFailures clears the consecutive-empty-first-sample streak.
// Called whenever a first decrypt after a context switch completes, which is
// the one observation that separates a slow instance from a wedged one. It
// leaves the steady-state timeouts alone; those mean something this does not
// disprove.
func (d *DecryptInstance) resetFirstSampleFailures() {
	d.healthMu.Lock()
	d.firstSampleTimeouts = d.firstSampleTimeouts[:0]
	d.healthMu.Unlock()
}

// observeWrapperTimeout records one timeout against the counter for its kind
// and reports whether the instance has now produced enough of them inside the
// window to be called wedged.
func (d *DecryptInstance) observeWrapperTimeout(now, cutoff time.Time, firstSample bool) bool {
	d.healthMu.Lock()
	defer d.healthMu.Unlock()
	counter := &d.timeouts
	if firstSample {
		counter = &d.firstSampleTimeouts
	}
	kept := (*counter)[:0]
	for _, at := range *counter {
		if !at.Before(cutoff) {
			kept = append(kept, at)
		}
	}
	*counter = append(kept, now)
	return len(*counter) >= wrapperTimeoutThreshold
}

// observeWrapperIOFailure feeds one local failure into the health rules.
//
// timeoutIsConclusive says whether exceeding this operation's budget is evidence
// about the process at all. It is true where the budget sits orders of magnitude
// above the whole observed distribution — a steady-state decrypt at 2s against a
// 4.751 ms worst case, a context-switch write at 2s against 131 µs.
//
// The first decrypt after a context switch is the hard case, because it is slow
// by nature: healthy instances have been measured past ten seconds, so the
// elapsed time alone says nothing. Feeding those to the general failure rule is
// a misfire waiting to happen — an album produces one such operation per track,
// which is exactly the spread of distinct connections and Adam IDs that rule
// looks for, so three slow tracks would restart a healthy wrapper.
//
// Whether any bytes came back separates the two populations cleanly, and the
// caller passes that in. A wedged wrapper on 2026-07-29 produced first-sample
// reads landing on the deadline to the millisecond — 30.000185s, 30.000409s,
// 30.000725s, 30.001491s, 30.001788s — with nothing read, while the same
// instance after a restart ran the identical album at 419 ms to 2.16 s. A slow
// operation completes, or at worst stalls partway through a reply; an operation
// that returns zero bytes after thirty seconds never got an answer at all. That
// is not slowness, and treating it as slowness is why both instances in that
// outage failed over to each other for an hour while Status kept reporting
// ready and nothing ever restarted them.
//
// So: a timeout with a partial read stays non-conclusive and feeds neither
// health rule. A timeout with an empty read is counted, and the existing
// wrapperTimeoutThreshold still requires two of them inside the window before
// the instance is declared wedged. The sample is not lost either way; the
// caller fails it over.
func (d *DecryptInstance) observeWrapperIOFailure(ctx context.Context, conn *decryptConn, adamID, stage string, timeoutIsConclusive bool, err error) {
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
	firstSample := stage == firstSampleStage
	if timedOut && !timeoutIsConclusive {
		logrus.Warnf("wrapper instance %s local %s exceeded its %s budget for Adam ID %s; failing the sample over without counting it against instance health: %v", d.id, stage, d.firstSampleTimeout, adamID, err)
		return
	}
	now := time.Now()
	if d.now != nil {
		now = d.now()
	}
	cutoff := now.Add(-wrapperFailureWindow)

	if timedOut && d.observeWrapperTimeout(now, cutoff, firstSample) {
		d.Unavailable(fmt.Sprintf(
			"%d local I/O timeouts in %s, most recently %s for Adam ID %s",
			wrapperTimeoutThreshold, wrapperFailureWindow, stage, adamID,
		))
		return
	}
	// An empty first sample feeds the streak rule above and stops there. The
	// general rule counts distinct connections and Adam IDs, and an album
	// produces exactly that spread — one first sample per track — so letting
	// these through would only move the misfire the streak rule was built to
	// avoid. Slowness is caught by the streak resetting on any completion;
	// nothing else about a first sample is evidence.
	if firstSample && timedOut {
		return
	}

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
		if timedOut {
			// Below the timeout threshold, so this instance is suspect rather than
			// condemned. The sample itself is not lost: the caller fails it over.
			logrus.Warnf("wrapper instance %s local %s I/O timed out (1/%d in %s) for Adam ID %s: %v", d.id, stage, wrapperTimeoutThreshold, wrapperFailureWindow, adamID, err)
			return
		}
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
	s.logLatency()
	_ = c.conn.SetDeadline(time.Time{})
	s.instance.releaseConn(c)
}

func (s *DecryptSession) Discard() {
	c, ok := s.takeConn()
	if !ok {
		return
	}
	s.logLatency()
	s.instance.discardConn(c)
}

func (d *DecryptInstance) setOperationDeadline(ctx context.Context, conn net.Conn, budget time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(budget)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	// Cancellation can race the future deadline above. Recheck after setting it
	// so an already-fired cancellation can never be extended to the budget.
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
func (d *DecryptInstance) decryptConn(ctx context.Context, c *decryptConn, sample, plaintext []byte, budget time.Duration) ([]byte, int, error) {
	if len(sample) == 0 {
		return nil, 0, errors.New("empty decrypt sample")
	}
	if len(plaintext) != len(sample) {
		return nil, 0, errors.New("plaintext buffer length does not match decrypt sample")
	}
	if err := d.setOperationDeadline(ctx, c.conn, budget); err != nil {
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
	// The write itself measures 25-131 µs; it is the read that follows on the
	// next decrypt that carries the wrapper's key setup, so the steady budget
	// is the right one here.
	if err := d.setOperationDeadline(ctx, c.conn, d.ioTimeout); err != nil {
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
