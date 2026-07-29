package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// This file is the supervisor's memory. The manager exists to run a decryptor
// that dies, and until now the only artefact a death left behind was the string
// "Wrapper Down": cmd.Wait() returned nil (the wrapper exits with status 0
// rather than crashing), and everything the wrapper printed went to log.Debug,
// which production never prints. Three rounds of fixes worked on detecting and
// recovering from a wedge because nothing here ever recorded why one happened.
const (
	// wrapperLogTail is how many of a wrapper's own output lines are kept per
	// instance so a death can be explained. Fifty covers a startup banner and
	// then some; the cost is one string slice per running wrapper.
	wrapperLogTail = 50
	// wrapperLogInfoBurst / wrapperLogInfoWindow cap how much wrapper output
	// reaches info, so that a future chatty wrapper cannot bury the manager's
	// own log. Muted lines still reach debug and still enter the tail.
	wrapperLogInfoBurst  = 120
	wrapperLogInfoWindow = time.Minute
)

// Crash-loop policy. Production on 2026-07-29 restarted one instance at
// 15:47:50, 15:48:08, 15:48:25 and 15:48:42 — four deaths in 52 seconds, each
// restarted straight back into the same immediate death, with nothing in the
// log to say it was happening.
const (
	// wrapperHealthyUptime separates an ordinary death from a startup death. A
	// wrapper that served for a full minute did not die on startup, so its
	// history is forgotten and it is restarted immediately: one restart and
	// everything is fine is the common case and must stay fast.
	wrapperHealthyUptime = 60 * time.Second
	// wrapperRestartWindow is how long consecutive short-lived deaths keep
	// counting against each other. A death ten minutes ago says nothing about
	// this one.
	wrapperRestartWindow = 10 * time.Minute
	// wrapperRestartLimit is how many consecutive short-lived deaths are
	// restarted before the instance is marked failed. The delays below add up
	// to 30s, and the observed loop burned ~17s of process life per attempt, so
	// the manager spends roughly two minutes trying — long enough to ride out
	// something transient, short enough that the failed state appears while an
	// operator is still looking at the incident.
	wrapperRestartLimit     = 5
	wrapperRestartBaseDelay = 2 * time.Second
	wrapperRestartMaxDelay  = 30 * time.Second
)

func wrapperTag(id string) string {
	return fmt.Sprintf("[wrapper %s]", strings.Split(id, "-")[0])
}

// wrapperProc is the per-process bookkeeping the supervisor needs the moment a
// wrapper dies: when it started, the last thing it said, and whether the
// manager asked it to stop. It hangs off WrapperInstance by pointer because
// WrapperInstance is copied by value when data/instances.json is loaded, so it
// must not itself contain a lock. Every method tolerates a nil receiver, since
// instances reconstructed by lookup helpers have no process behind them.
type wrapperProc struct {
	mu              sync.Mutex
	startedAt       time.Time
	tail            []string
	dropped         int
	stopRequested   bool
	infoWindowStart time.Time
	infoCount       int
	infoMuted       bool
}

func newWrapperProc() *wrapperProc {
	return &wrapperProc{tail: make([]string, 0, wrapperLogTail)}
}

func (p *wrapperProc) markStarted(at time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.startedAt = at
	p.mu.Unlock()
}

// uptime reports how long the process had been running, or zero if it never
// started.
func (p *wrapperProc) uptime(now time.Time) time.Duration {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.startedAt.IsZero() {
		return 0
	}
	return now.Sub(p.startedAt)
}

func (p *wrapperProc) markStopRequested() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.stopRequested = true
	p.mu.Unlock()
}

func (p *wrapperProc) stopWasRequested() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopRequested
}

func (p *wrapperProc) recordLine(line string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.tail) >= wrapperLogTail {
		copy(p.tail, p.tail[len(p.tail)-wrapperLogTail+1:])
		p.tail = p.tail[:wrapperLogTail-1]
		p.dropped++
	}
	p.tail = append(p.tail, line)
}

// tailLines returns the kept lines oldest first, and how many older ones were
// evicted.
func (p *wrapperProc) tailLines() ([]string, int) {
	if p == nil {
		return nil, 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.tail...), p.dropped
}

// allowInfo answers whether this line may go to info, and reports the single
// transition into muting so it can be said out loud once.
func (p *wrapperProc) allowInfo(now time.Time) (allow, justMuted bool) {
	if p == nil {
		return true, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.infoWindowStart.IsZero() || now.Sub(p.infoWindowStart) >= wrapperLogInfoWindow {
		p.infoWindowStart = now
		p.infoCount = 0
		p.infoMuted = false
	}
	p.infoCount++
	if p.infoCount <= wrapperLogInfoBurst {
		return true, false
	}
	if !p.infoMuted {
		p.infoMuted = true
		return false, true
	}
	return false, false
}

// isWrapperNoise names the two output families someone already judged not worth
// reading. The condition this replaces was
//
//	!strings.HasPrefix(line, "__") || !strings.HasPrefix(line, "WARNING")
//
// which is always true, because a line cannot start with both: the intent was
// &&, i.e. "everything except these two". It never showed, because the whole
// branch logged at debug and production runs at info.
func isWrapperNoise(line string) bool {
	return strings.HasPrefix(line, "__") || strings.HasPrefix(line, "WARNING")
}

// logWrapperLine decides how visible one line of wrapper output is.
//
// The deliberate choice: anything that is not one of the two known-noisy
// families goes to info. The wrapper's own words are the diagnostic this
// service has been missing, only four fixed strings were ever acted on, and
// everything else — including whatever it says on its way out — was dropped.
// wrapperLogInfoBurst is the safety valve if a wrapper release turns chatty;
// muted lines still reach debug and still enter the tail a death prints.
func logWrapperLine(instance *WrapperInstance, line string) {
	tag := wrapperTag(instance.Id)
	if isWrapperNoise(line) {
		log.Debugf("%s %s", tag, line)
		return
	}
	allow, justMuted := instance.proc.allowInfo(time.Now())
	if justMuted {
		log.Warnf("%s printed more than %d lines in %s; the rest of this window is demoted to debug (they are still kept for the exit report)", tag, wrapperLogInfoBurst, wrapperLogInfoWindow)
	}
	if !allow {
		log.Debugf("%s %s", tag, line)
		return
	}
	log.Infof("%s %s", tag, line)
}

// describeExitStatus turns a finished process into one readable phrase.
//
// A clean exit is called out explicitly because it is the finding that explains
// the whole incident: `Wrapper exited with error` never appeared in 24 hours of
// production logs while `Wrapper Down` appeared four times, so cmd.Wait()
// returned nil every time — the wrapper is not crashing, it is choosing to
// leave.
func describeExitStatus(instance *WrapperInstance, waitErr error) string {
	if instance.Cmd == nil || instance.Cmd.ProcessState == nil {
		if waitErr != nil {
			return fmt.Sprintf("no exit status (%v)", waitErr)
		}
		return "no exit status"
	}
	state := instance.Cmd.ProcessState
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return fmt.Sprintf("killed by signal %d (%s)", int(status.Signal()), status.Signal())
	}
	code := state.ExitCode()
	if code == 0 {
		return "exit status 0, a clean exit rather than a crash"
	}
	plain := fmt.Sprintf("exit status %d", code)
	if waitErr != nil && waitErr.Error() != plain {
		return fmt.Sprintf("%s (%v)", plain, waitErr)
	}
	return plain
}

// logWrapperExit records everything known about one death at info: which
// instance, how it ended, how long it had been up, whether the manager asked
// for it, and the last lines the wrapper printed on its way out.
func logWrapperExit(instance *WrapperInstance, waitErr error) {
	tag := wrapperTag(instance.Id)
	uptime := instance.proc.uptime(time.Now()).Round(time.Millisecond)
	intent := "the manager did not ask it to stop"
	if instance.proc.stopWasRequested() {
		intent = "the manager had asked it to stop"
	}
	tail, dropped := instance.proc.tailLines()
	dropNote := ""
	if dropped > 0 {
		dropNote = fmt.Sprintf(", %d earlier lines dropped", dropped)
	}
	log.Infof("%s wrapper exited: %s; up %s; %s; last %d lines follow%s",
		tag, describeExitStatus(instance, waitErr), uptime, intent, len(tail), dropNote)
	if len(tail) == 0 {
		log.Infof("%s wrapper printed nothing before exiting", tag)
		return
	}
	for i, line := range tail {
		log.Infof("%s exit tail %d/%d: %s", tag, i+1, len(tail), line)
	}
}

type restartRecord struct {
	deaths int
	last   time.Time
}

type restartDecision struct {
	restart bool
	delay   time.Duration
	// deaths is how many consecutive short-lived deaths this instance has now
	// had, this one included.
	deaths     int
	shortLived bool
}

// restartPolicy decides whether a dead wrapper is restarted, and how soon.
// Before it existed, wrapperDown restarted unconditionally, so an instance that
// died immediately on startup was restarted into the same immediate death for
// as long as the manager ran, with no trace beyond one "Wrapper Down" per lap.
type restartPolicy struct {
	mu      sync.Mutex
	records map[string]*restartRecord
}

func newRestartPolicy() *restartPolicy {
	return &restartPolicy{records: make(map[string]*restartRecord)}
}

// plan records one death and answers what to do about it.
//
// A death after wrapperHealthyUptime clears the history: whatever killed the
// wrapper, it was not failing to start, and the next one deserves the same
// prompt restart the first would have got. Deaths that a health rule caused
// still count — an instance condemned as wedged within a minute of starting,
// five times running, is not going to fix itself either.
func (p *restartPolicy) plan(id string, uptime time.Duration, now time.Time) restartDecision {
	p.mu.Lock()
	defer p.mu.Unlock()
	shortLived := uptime < wrapperHealthyUptime
	record := p.records[id]
	if record == nil || !shortLived || now.Sub(record.last) > wrapperRestartWindow {
		record = &restartRecord{}
		p.records[id] = record
	}
	record.deaths++
	record.last = now

	decision := restartDecision{deaths: record.deaths, shortLived: shortLived}
	if record.deaths > wrapperRestartLimit {
		return decision
	}
	decision.restart = true
	decision.delay = restartBackoff(record.deaths)
	return decision
}

// restartBackoff is 0s, 2s, 4s, 8s, 16s, capped at wrapperRestartMaxDelay. The
// first restart is immediate on purpose: a single ordinary death must recover
// as promptly as it did before there was a policy at all.
func restartBackoff(deaths int) time.Duration {
	if deaths <= 1 {
		return 0
	}
	delay := wrapperRestartBaseDelay << (deaths - 2)
	if delay <= 0 || delay > wrapperRestartMaxDelay {
		return wrapperRestartMaxDelay
	}
	return delay
}

// WrapperFailure is an instance the supervisor has stopped restarting.
type WrapperFailure struct {
	Id       string    `json:"id"`
	Account  string    `json:"account"`
	Reason   string    `json:"reason"`
	At       time.Time `json:"at"`
	Deaths   int       `json:"deaths"`
	Restarts int       `json:"restarts"`
}

// failureRegistry keeps abandoned instances visible. Without it, giving up
// would be even quieter than the hot loop it replaces.
type failureRegistry struct {
	mu       sync.Mutex
	failures map[string]WrapperFailure
}

func newFailureRegistry() *failureRegistry {
	return &failureRegistry{failures: make(map[string]WrapperFailure)}
}

func (r *failureRegistry) mark(failure WrapperFailure) {
	r.mu.Lock()
	r.failures[failure.Id] = failure
	r.mu.Unlock()
}

func (r *failureRegistry) clear(id string) {
	r.mu.Lock()
	delete(r.failures, id)
	r.mu.Unlock()
}

func (r *failureRegistry) list() []WrapperFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	failures := make([]WrapperFailure, 0, len(r.failures))
	for _, failure := range r.failures {
		failures = append(failures, failure)
	}
	return failures
}

func (r *failureRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures)
}

var (
	wrapperRestarts = newRestartPolicy()
	wrapperFailures = newFailureRegistry()
)

// FailedWrappers lists the instances the supervisor has given up on.
func FailedWrappers() []WrapperFailure { return wrapperFailures.list() }

// describeFailedWrappers renders those instances for the status log.
func describeFailedWrappers() string {
	failures := wrapperFailures.list()
	if len(failures) == 0 {
		return "none"
	}
	described := make([]string, 0, len(failures))
	for _, failure := range failures {
		described = append(described, fmt.Sprintf("%s (%s)", failure.Id, failure.Reason))
	}
	sort.Strings(described)
	return strings.Join(described, "; ")
}

// startWrapper is the restart entry point, indirected so tests can observe what
// the policy decided without launching a wrapper process. It is assigned in
// init rather than inline because WrapperStart leads back to wrapperDown, which
// calls this, and Go rejects that as an initialization cycle.
var startWrapper func(id, account string, delay time.Duration)

func init() {
	startWrapper = func(id, account string, delay time.Duration) {
		go func() {
			if delay > 0 {
				time.Sleep(delay)
			}
			WrapperStart(id, account)
		}()
	}
}

// abandonReplacement tells the dispatcher that a wrapper it is waiting on is
// not coming back, so a condemnation it is holding open can proceed.
func abandonReplacement(id, reason string) {
	if WMDispatcher == nil {
		return
	}
	WMDispatcher.ReplacementFailed(id, reason)
}
