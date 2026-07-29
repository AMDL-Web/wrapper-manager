package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// syncBuffer collects log output. logrus serialises its own writes, but a
// goroutine left over from another test may still be logging while the test
// body reads, so reads are locked too.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog redirects the standard logger at production level. Level matters:
// the bug these tests cover is that everything the wrapper said went to debug,
// which production never prints, so a test that captured debug would pass
// against the broken code.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	logger := log.StandardLogger()
	previousOut := logger.Out
	previousLevel := logger.GetLevel()
	logger.SetOutput(buf)
	logger.SetLevel(log.InfoLevel)
	t.Cleanup(func() {
		logger.SetOutput(previousOut)
		logger.SetLevel(previousLevel)
	})
	return buf
}

func testInstance(id string) *WrapperInstance {
	return &WrapperInstance{Id: id, Account: "someone@example.com", proc: newWrapperProc()}
}

// TestWrapperOutputIsRecordedAndOnlyNoiseIsHidden covers the always-true
// condition in handleOutput: `!HasPrefix(line, "__") || !HasPrefix(line,
// "WARNING")` can never be false, and the branch it guarded logged at debug, so
// at production level nothing the wrapper said was ever printed.
func TestWrapperOutputIsRecordedAndOnlyNoiseIsHidden(t *testing.T) {
	buf := captureLog(t)
	instance := testInstance("4f2c9a1b-0000-4000-8000-000000000001")

	handleOutput(strings.NewReader(
		"__internal chatter\n"+
			"WARNING: deprecated flag\n"+
			"something the wrapper wanted to say\n"), instance)

	tail, dropped := instance.proc.tailLines()
	if len(tail) != 3 || dropped != 0 {
		t.Fatalf("tail = %#v (dropped %d), want all three lines kept", tail, dropped)
	}
	out := buf.String()
	if !strings.Contains(out, "something the wrapper wanted to say") {
		t.Fatal("a line that is neither noise nor a known marker was discarded instead of logged")
	}
	if !strings.Contains(out, "[wrapper 4f2c9a1b]") {
		t.Fatalf("wrapper output was not tagged with its instance: %s", out)
	}
	if strings.Contains(out, "__internal chatter") || strings.Contains(out, "deprecated flag") {
		t.Fatal("known-noise lines reached info; the prefix filter still does not mean what it says")
	}
}

func TestWrapperOutputTailKeepsTheLastLines(t *testing.T) {
	proc := newWrapperProc()
	for i := 0; i < wrapperLogTail+12; i++ {
		proc.recordLine(fmt.Sprintf("line %d", i))
	}
	tail, dropped := proc.tailLines()
	if len(tail) != wrapperLogTail {
		t.Fatalf("tail length = %d, want %d", len(tail), wrapperLogTail)
	}
	if dropped != 12 {
		t.Fatalf("dropped = %d, want 12", dropped)
	}
	if want := fmt.Sprintf("line %d", wrapperLogTail+11); tail[len(tail)-1] != want {
		t.Fatalf("last kept line = %q, want %q", tail[len(tail)-1], want)
	}
	if want := "line 12"; tail[0] != want {
		t.Fatalf("first kept line = %q, want %q", tail[0], want)
	}
}

// TestWrapperOutputIsMutedAfterABurst checks the safety valve: a chatty wrapper
// must not be able to bury the manager's own log, and the lines it mutes must
// still be there for the exit report.
func TestWrapperOutputIsMutedAfterABurst(t *testing.T) {
	buf := captureLog(t)
	instance := testInstance("chatty-0000-4000-8000-000000000001")
	total := wrapperLogInfoBurst + 5
	var output strings.Builder
	for i := 0; i < total; i++ {
		fmt.Fprintf(&output, "line %d\n", i)
	}
	handleOutput(strings.NewReader(output.String()), instance)

	out := buf.String()
	if !strings.Contains(out, "line 0") {
		t.Fatal("output was muted before the burst allowance was used")
	}
	if !strings.Contains(out, "the rest of this window is demoted to debug") {
		t.Fatal("muting happened silently")
	}
	last := fmt.Sprintf("line %d", total-1)
	if strings.Contains(out, last) {
		t.Fatalf("line past the burst allowance still reached info: %s", last)
	}
	if count := strings.Count(out, "demoted to debug"); count != 1 {
		t.Fatalf("muting was announced %d times, want once per window", count)
	}
	tail, _ := instance.proc.tailLines()
	if tail[len(tail)-1] != last {
		t.Fatalf("muted line %q was not kept in the tail (last kept %q)", last, tail[len(tail)-1])
	}
}

// exitedProcess runs the test binary's helper with no mode set, so it returns
// immediately and the process exits 0 — the shape production actually produces.
func exitedProcess(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestWrapperProcessHelper")
	cmd.Env = append(os.Environ(), "WRAPPER_MANAGER_HELPER_MODE=")
	if err := cmd.Run(); err != nil {
		t.Fatalf("helper process did not exit cleanly: %v", err)
	}
	return cmd
}

// TestWrapperExitLogsACleanDeathWithItsLastWords is the whole point of the
// change. Production logged one string, "Wrapper Down", for an event whose
// cause nobody could find for three rounds of fixes.
func TestWrapperExitLogsACleanDeathWithItsLastWords(t *testing.T) {
	buf := captureLog(t)
	instance := testInstance("4f2c9a1b-0000-4000-8000-000000000002")
	instance.Cmd = exitedProcess(t)
	instance.proc.markStarted(time.Now().Add(-17400 * time.Millisecond))
	instance.proc.recordLine("[!] listening m3u8 request on 0.0.0.0:34761")
	instance.proc.recordLine("[!] decrypt session closed")

	logWrapperExit(instance, nil)

	out := buf.String()
	for _, want := range []string{
		"[wrapper 4f2c9a1b]",
		"exit status 0, a clean exit rather than a crash",
		"up 17.4",
		"the manager did not ask it to stop",
		"exit tail 1/2: [!] listening m3u8 request on 0.0.0.0:34761",
		"exit tail 2/2: [!] decrypt session closed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("exit log is missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "level=info") {
		t.Fatalf("exit log did not reach info level:\n%s", out)
	}
}

func TestWrapperExitLogsSilenceExplicitly(t *testing.T) {
	buf := captureLog(t)
	instance := testInstance("quiet-0000-4000-8000-000000000003")
	instance.Cmd = exitedProcess(t)

	logWrapperExit(instance, nil)

	if !strings.Contains(buf.String(), "wrapper printed nothing before exiting") {
		t.Fatalf("an exit with no output said nothing about it:\n%s", buf.String())
	}
}

// TestWrapperExitLogsASignalAndWhoAskedForIt separates the deaths the manager
// caused from the ones it is investigating.
func TestWrapperExitLogsASignalAndWhoAskedForIt(t *testing.T) {
	buf := captureLog(t)
	instance := startWrapperProcessHelper(t, "ignore")
	instance.proc = newWrapperProc()
	instance.proc.markStarted(time.Now())

	if err := terminateWrapperInstance(instance, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	<-instance.Done
	logWrapperExit(instance, nil)

	out := buf.String()
	if !strings.Contains(out, "killed by signal 9") {
		t.Fatalf("a signalled exit was not reported as one:\n%s", out)
	}
	if !strings.Contains(out, "the manager had asked it to stop") {
		t.Fatalf("a manager-requested stop was not distinguished from a spontaneous exit:\n%s", out)
	}
}

func TestOrdinaryDeathRestartsImmediately(t *testing.T) {
	policy := newRestartPolicy()
	now := time.Now()
	for i := 0; i < 3; i++ {
		decision := policy.plan("a", 4*time.Hour, now.Add(time.Duration(i)*time.Minute))
		if !decision.restart || decision.delay != 0 || decision.deaths != 1 {
			t.Fatalf("death %d after a long run: %+v, want an immediate restart", i, decision)
		}
		if decision.shortLived {
			t.Fatal("a wrapper that ran for hours was classified as short-lived")
		}
	}
}

// TestCrashLoopBacksOffThenGivesUp is the 2026-07-29 sequence: an instance that
// died at 15:47:50, 15:48:08, 15:48:25 and 15:48:42, each time restarted into
// the same immediate death.
func TestCrashLoopBacksOffThenGivesUp(t *testing.T) {
	policy := newRestartPolicy()
	now := time.Now()
	wantDelays := []time.Duration{0, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if len(wantDelays) != wrapperRestartLimit {
		t.Fatalf("test covers %d restarts but the limit is %d", len(wantDelays), wrapperRestartLimit)
	}
	for i, want := range wantDelays {
		now = now.Add(17 * time.Second)
		decision := policy.plan("a", 17*time.Second, now)
		if !decision.restart {
			t.Fatalf("restart %d was refused, want the policy to keep trying up to the limit", i+1)
		}
		if decision.delay != want {
			t.Fatalf("restart %d delay = %s, want %s", i+1, decision.delay, want)
		}
	}
	now = now.Add(17 * time.Second)
	decision := policy.plan("a", 17*time.Second, now)
	if decision.restart {
		t.Fatal("the policy restarted a wrapper that had died short-lived past the limit")
	}
	if decision.deaths != wrapperRestartLimit+1 {
		t.Fatalf("deaths = %d, want %d", decision.deaths, wrapperRestartLimit+1)
	}
}

func TestHealthyUptimeClearsTheCrashLoopHistory(t *testing.T) {
	policy := newRestartPolicy()
	now := time.Now()
	for i := 0; i < 3; i++ {
		now = now.Add(time.Second)
		policy.plan("a", time.Second, now)
	}
	now = now.Add(wrapperHealthyUptime + time.Second)
	decision := policy.plan("a", wrapperHealthyUptime+time.Second, now)
	if !decision.restart || decision.delay != 0 || decision.deaths != 1 {
		t.Fatalf("decision after a healthy run = %+v, want the history cleared and an immediate restart", decision)
	}
}

func TestDeathsOutsideTheWindowDoNotAccumulate(t *testing.T) {
	policy := newRestartPolicy()
	now := time.Now()
	for i := 0; i < wrapperRestartLimit+3; i++ {
		now = now.Add(wrapperRestartWindow + time.Minute)
		decision := policy.plan("a", time.Second, now)
		if !decision.restart || decision.delay != 0 {
			t.Fatalf("death %d spaced beyond the window = %+v, want an immediate restart", i, decision)
		}
	}
}

func TestRestartPolicyTracksInstancesSeparately(t *testing.T) {
	policy := newRestartPolicy()
	now := time.Now()
	for i := 0; i < wrapperRestartLimit+1; i++ {
		policy.plan("a", time.Second, now.Add(time.Duration(i)*time.Second))
	}
	decision := policy.plan("b", time.Second, now)
	if !decision.restart || decision.deaths != 1 {
		t.Fatalf("second instance decision = %+v, want an unrelated instance's history ignored", decision)
	}
}

// isolateSupervisor swaps the supervisor's package state for this test.
func isolateSupervisor(t *testing.T) *Dispatcher {
	t.Helper()
	previousRestarts, previousFailures := wrapperRestarts, wrapperFailures
	previousStart, previousDispatcher, previousInstances := startWrapper, WMDispatcher, Instances
	wrapperRestarts = newRestartPolicy()
	wrapperFailures = newFailureRegistry()
	Instances = nil
	WMDispatcher = NewDispatcher()
	t.Cleanup(func() {
		wrapperRestarts, wrapperFailures = previousRestarts, previousFailures
		startWrapper, WMDispatcher, Instances = previousStart, previousDispatcher, previousInstances
	})
	return WMDispatcher
}

// TestWrapperDownStopsRestartingACrashLoop drives the supervisor's restart path
// end to end. Before this, wrapperDown was `if !instance.NoRestart { go
// WrapperStart(...) }` — unconditional, so a wrapper dying on startup was
// restarted into the same death forever, and the only trace was one "Wrapper
// Down" per lap.
func TestWrapperDownStopsRestartingACrashLoop(t *testing.T) {
	captureLog(t)
	d := isolateSupervisor(t)

	// A wedged instance elsewhere in the pool is being held in service waiting
	// for exactly this replacement.
	d.mu.Lock()
	d.pendingReplacements["crashy"] = d.clock()
	d.mu.Unlock()

	var mu sync.Mutex
	var delays []time.Duration
	startWrapper = func(id, account string, delay time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		if id != "crashy" || account != "someone@example.com" {
			t.Errorf("restart of %s/%s, want the same id and account", id, account)
		}
		delays = append(delays, delay)
	}

	for i := 0; i < wrapperRestartLimit+1; i++ {
		instance := testInstance("crashy")
		instance.proc.markStarted(time.Now().Add(-17 * time.Second))
		wrapperDown(instance, nil)
	}

	mu.Lock()
	got := append([]time.Duration(nil), delays...)
	mu.Unlock()
	want := []time.Duration{0, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("restarts = %v, want %v — the supervisor must stop after %d", got, want, wrapperRestartLimit)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("restart %d delay = %s, want %s", i+1, got[i], want[i])
		}
	}

	failures := FailedWrappers()
	if len(failures) != 1 || failures[0].Id != "crashy" {
		t.Fatalf("failed wrappers = %#v, want the abandoned instance recorded", failures)
	}
	if !strings.Contains(failures[0].Reason, "crash loop") || failures[0].Deaths != wrapperRestartLimit+1 {
		t.Fatalf("failure record = %#v, want a crash-loop reason and the death count", failures[0])
	}
	if !strings.Contains(describeFailedWrappers(), "crashy") {
		t.Fatalf("status log line = %q, want the failed instance named", describeFailedWrappers())
	}

	// And the instance it was replacing must no longer be pinned in service.
	d.mu.RLock()
	_, stillPending := d.pendingReplacements["crashy"]
	d.mu.RUnlock()
	if stillPending {
		t.Fatal("giving up on a replacement left the condemnation gate shut")
	}
}

// TestWrapperDownRestartsAnOrdinaryDeathImmediately keeps the common case fast:
// one wrapper dies after a long healthy run, one restart, no backoff.
func TestWrapperDownRestartsAnOrdinaryDeathImmediately(t *testing.T) {
	captureLog(t)
	isolateSupervisor(t)

	restarted := make(chan time.Duration, 1)
	startWrapper = func(_, _ string, delay time.Duration) { restarted <- delay }

	instance := testInstance("steady")
	instance.proc.markStarted(time.Now().Add(-6 * time.Hour))
	wrapperDown(instance, nil)

	select {
	case delay := <-restarted:
		if delay != 0 {
			t.Fatalf("ordinary death restart delay = %s, want none", delay)
		}
	default:
		t.Fatal("an ordinary death was not restarted")
	}
	if len(FailedWrappers()) != 0 {
		t.Fatalf("failed wrappers = %#v, want none", FailedWrappers())
	}
}

func TestWrapperReadyClearsAFailureMark(t *testing.T) {
	isolateSupervisor(t)
	wrapperFailures.mark(WrapperFailure{Id: "recovered", Reason: "crash loop"})
	wrapperFailures.clear("recovered")
	if got := wrapperFailures.count(); got != 0 {
		t.Fatalf("failed wrappers = %d after the instance came back, want 0", got)
	}
}
