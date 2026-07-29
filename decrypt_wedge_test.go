package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// backendWrapperTimeout is what amdl-backend's wrapper.timeout_seconds is set to
// in production. It is not the manager's to change, and the manager's own
// first-sample budget only works if it is strictly smaller.
const backendWrapperTimeout = 30 * time.Second

// The two 30-second budgets collided, the backend's clock started ~260ms-1.7s
// earlier, and so the manager's entire first-sample health apparatus was
// unreachable: forty-two consecutive stalls on a dead wrapper, zero
// condemnations, zero log lines. Anything that raises this back into the
// backend's budget restores that blindness silently, so pin it.
func TestFirstSampleBudgetFiresBeforeTheBackendGivesUp(t *testing.T) {
	if defaultFirstSampleIOTimeout >= backendWrapperTimeout {
		t.Fatalf("first-sample budget %s does not fire before the backend's %s, so every stall is cancelled from above and the health rules never see it",
			defaultFirstSampleIOTimeout, backendWrapperTimeout)
	}
	// The lead-in measured across 42 production stalls was 260ms-1.702s. Margin
	// has to clear the worst of those with room, or the collision comes back
	// under load.
	const worstObservedLeadIn = 1702 * time.Millisecond
	if margin := backendWrapperTimeout - defaultFirstSampleIOTimeout; margin < 2*worstObservedLeadIn {
		t.Fatalf("margin below the backend is %s, under 2x the worst measured lead-in (%s)", margin, worstObservedLeadIn)
	}
	// And it must still clear the real population: the slowest first sample from
	// a healthy instance is 10.02s.
	const worstHealthyFirstSample = 10020 * time.Millisecond
	if defaultFirstSampleIOTimeout < 2*worstHealthyFirstSample {
		t.Fatalf("first-sample budget %s is under 2x the slowest healthy key setup (%s); it will condemn instances for being slow",
			defaultFirstSampleIOTimeout, worstHealthyFirstSample)
	}
}

func newHealthFixture(t *testing.T) (*DecryptInstance, chan struct{}) {
	t.Helper()
	instance := &DecryptInstance{
		id:                 "test",
		connections:        make(map[*decryptConn]struct{}),
		poolLimit:          maxPoolSize,
		firstSampleTimeout: defaultFirstSampleIOTimeout,
		now:                time.Now,
	}
	terminated := make(chan struct{}, 4)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}
	return instance, terminated
}

func timeoutErr() error { return &net.OpError{Op: "read", Err: &timeoutError{}} }

// The guard in classifyLocalWrapperIOError discards everything whose context is
// already cancelled, which is right for a client that went away — and is exactly
// what swallowed all 42 wedge events, because the backend's timeout always won
// the race. A first sample that returned nothing after fifteen seconds is
// evidence no matter who stopped waiting first.
func TestCancelledEmptyFirstSampleStillReachesTheHealthRules(t *testing.T) {
	for _, tc := range []struct {
		name       string
		elapsed    time.Duration
		conclusive bool
		wantWedged bool
	}{
		{
			name:       "nothing read, past the evidence threshold: counts",
			elapsed:    firstSampleCancelEvidence,
			conclusive: true,
			wantWedged: true,
		},
		{
			name:       "nothing read, but the client gave up early: still says nothing",
			elapsed:    firstSampleCancelEvidence - time.Second,
			conclusive: true,
			wantWedged: false,
		},
		{
			// The empty-vs-partial distinction is the one the health rules were
			// rebuilt around, and widening the cancellation path must not erode
			// it. Bytes came back, so the wrapper is talking.
			name:       "bytes came back before the cancellation: still says nothing",
			elapsed:    2 * firstSampleCancelEvidence,
			conclusive: false,
			wantWedged: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, terminated := newHealthFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			for i := 0; i < wrapperTimeoutThreshold; i++ {
				instance.observeWrapperIOFailure(ctx, &decryptConn{},
					fmt.Sprintf("adam-%d", i), firstSampleStage, tc.conclusive, tc.elapsed, timeoutErr())
			}

			instance.poolMu.Lock()
			wedged := instance.isClosed
			instance.poolMu.Unlock()
			if wedged != tc.wantWedged {
				t.Fatalf("instance condemned = %v, want %v", wedged, tc.wantWedged)
			}
			if !tc.wantWedged {
				select {
				case <-terminated:
					t.Fatal("a cancelled client restarted a healthy wrapper")
				default:
				}
			}
		})
	}
}

// A cancellation on a steady-state decrypt keeps the old meaning: the budget
// there is ~400x the worst sample, so the manager owns it and a client giving up
// says nothing about the wrapper.
func TestCancelledSteadyStateDecryptStillSaysNothing(t *testing.T) {
	instance, terminated := newHealthFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < wrapperTimeoutThreshold+2; i++ {
		instance.observeWrapperIOFailure(ctx, &decryptConn{},
			fmt.Sprintf("adam-%d", i), "decrypt", true, time.Minute, timeoutErr())
	}
	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if instance.isClosed {
		t.Fatal("a cancelled steady-state decrypt condemned the instance")
	}
	select {
	case <-terminated:
		t.Fatal("a cancelled steady-state decrypt restarted the wrapper")
	default:
	}
}

func TestKeySetupWitnessTracksTheLock(t *testing.T) {
	var w wrapperKeySetupWitness
	now := time.Now()

	if _, _, ok := w.stranded(); ok {
		t.Fatal("a fresh witness reports the lock as stranded")
	}
	w.observeLine("[.] adamId: 1579273516, uri: skd://itunes.apple.com/p1250957839/c23", now)
	if got := w.announces.Load(); got != 1 {
		t.Fatalf("announces = %d, want 1", got)
	}
	if _, _, ok := w.stranded(); ok {
		t.Fatal("an announce left the lock reported as stranded")
	}

	w.observeLine("[!] catched an exception: Invalid CKC error. ", now)
	line, at, ok := w.stranded()
	if !ok {
		t.Fatal("an exception with no announce after it is not reported as stranded")
	}
	if !at.Equal(now) {
		t.Fatalf("stranded at %s, want %s", at, now)
	}
	if line == "" {
		t.Fatal("the exception text was not kept for the condemnation message")
	}

	// A second throw must not overwrite the first: the first one is the throw
	// that leaked the lock.
	w.observeLine("[!] catched an exception: something else", now.Add(time.Second))
	if again, _, _ := w.stranded(); again != line {
		t.Fatalf("stranded reason changed to %q, want the first throw %q", again, line)
	}

	// A wrapper that takes the lock again plainly did not leak it.
	w.observeLine("[.] adamId: 42, uri: skd://x", now.Add(2*time.Second))
	if _, _, ok := w.stranded(); ok {
		t.Fatal("an announce after the exception did not clear the stranded state")
	}
}

// The direct probe. Readiness stayed true throughout the outage and the I/O
// error said nothing, so this is the only signal that names the fault.
func TestSilentKeySetupCondemnsOnlyTheRealWedge(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		lines                 []string
		announceDuringAttempt bool
		wantWedged            bool
	}{
		{
			name:       "threw, and has not taken the lock since",
			lines:      []string{"[.] adamId: 1, uri: skd://a", "[!] catched an exception: Invalid CKC error. "},
			wantWedged: true,
		},
		{
			// Slow, or simply queued behind another session that holds the lock
			// legitimately. Nothing has thrown, so nothing has leaked.
			name:       "no exception: just slow, must not condemn",
			lines:      []string{"[.] adamId: 1, uri: skd://a"},
			wantWedged: false,
		},
		{
			name:       "threw but recovered and announced again",
			lines:      []string{"[!] catched an exception: Invalid CKC error. ", "[.] adamId: 2, uri: skd://b"},
			wantWedged: false,
		},
		{
			// The lock is moving, so it is not held forever — whatever went wrong
			// with this particular attempt, it is not the leak.
			name:                  "threw, but the lock is being acquired during the attempt",
			lines:                 []string{"[!] catched an exception: Invalid CKC error. "},
			announceDuringAttempt: true,
			wantWedged:            false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance, terminated := newHealthFixture(t)
			for _, line := range tc.lines {
				instance.ObserveWrapperLine(line)
			}
			before := instance.keySetupAnnounces()
			if tc.announceDuringAttempt {
				instance.ObserveWrapperLine("[.] adamId: 3, uri: skd://c")
			}

			instance.observeSilentKeySetup(before, "adam-1", 29*time.Second)

			instance.poolMu.Lock()
			wedged := instance.isClosed
			instance.poolMu.Unlock()
			if wedged != tc.wantWedged {
				t.Fatalf("instance condemned = %v, want %v", wedged, tc.wantWedged)
			}
			if tc.wantWedged {
				select {
				case <-terminated:
				case <-time.After(time.Second):
					t.Fatal("a wedged wrapper was not restarted, and a leaked lock has no other recovery")
				}
			}
		})
	}
}

// End to end through Decrypt, against a wrapper that behaves exactly as the
// wedged one did: it accepts the connection, it never announces, and it never
// answers. The client's context is cancelled first, as the backend's shorter
// effective timeout did in production, so this also pins that the verdict does
// not depend on the manager owning the deadline.
func TestWedgedWrapperIsCondemnedThroughDecryptEvenWhenTheClientGivesUpFirst(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "wedged", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instance.Close)
	instance.ioTimeout = 5 * time.Second
	instance.firstSampleTimeout = 5 * time.Second
	terminated := make(chan struct{}, 4)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}

	// The wrapper took an exception out of its key-setup path and has not
	// touched the lock since.
	instance.ObserveWrapperLine("[.] adamId: 1, uri: skd://a")
	instance.ObserveWrapperLine("[!] catched an exception: Invalid CKC error. ")

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	session, err := instance.OpenSession(ctx, "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	// "block" never answers, and the client's deadline expires long before the
	// manager's — the production shape exactly.
	if _, err := session.Decrypt("song-1", "key-1", []byte("block")); err == nil {
		t.Fatal("expected the wedged first sample to fail")
	}

	select {
	case <-terminated:
	case <-time.After(2 * time.Second):
		t.Fatal("a wedged wrapper survived a client-cancelled first sample, which is how the fault stayed invisible for forty minutes")
	}
	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if !instance.isClosed {
		t.Fatal("the wedged instance was left in service")
	}
}

// A healthy wrapper that has never thrown must survive the same shape of
// failure, because a client cancelling early is ordinary.
func TestHealthyWrapperSurvivesAClientCancelledFirstSample(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "healthy", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instance.Close)
	instance.ioTimeout = 5 * time.Second
	instance.firstSampleTimeout = 5 * time.Second
	terminated := make(chan struct{}, 4)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}
	instance.ObserveWrapperLine("[.] adamId: 1, uri: skd://a")

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	session, err := instance.OpenSession(ctx, "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.Decrypt("song-1", "key-1", []byte("block")); err == nil {
		t.Fatal("expected the blocked sample to fail")
	}

	select {
	case <-terminated:
		t.Fatal("a client cancelling early restarted a wrapper that never threw")
	case <-time.After(200 * time.Millisecond):
	}
	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if instance.isClosed {
		t.Fatal("a client cancelling early quarantined a healthy instance")
	}
}

// The latency log is what the diagnostic reads. A first sample that failed is
// time-to-failure, not a slow key setup, and the two must not share a
// population — that conflation is what produced a 29.5s "latency" curve out of
// events that decrypted nothing.
func TestFailedFirstSampleIsNotRecordedAsKeySetupLatency(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instance.Close)
	instance.ioTimeout = 60 * time.Millisecond
	instance.firstSampleTimeout = 60 * time.Millisecond
	instance.terminateWrapper = func() error { return nil }

	session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Decrypt("song-1", "key-1", []byte("block")); err == nil {
		t.Fatal("expected the first sample to fail")
	}
	if session.firstLatency.count != 0 {
		t.Fatalf("a failed first sample was recorded as key-setup latency (n=%d, max=%s)",
			session.firstLatency.count, time.Duration(session.firstLatency.maxNS))
	}
	if session.firstFailedLatency.count != 1 {
		t.Fatalf("failed first samples recorded = %d, want 1", session.firstFailedLatency.count)
	}
	session.Discard()

	// And a successful one still lands in the latency population.
	ok, err := instance.OpenSession(context.Background(), "song-2", "key-2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ok.Decrypt("song-2", "key-2", []byte("fine")); err != nil {
		t.Fatal(err)
	}
	if ok.firstLatency.count != 1 {
		t.Fatalf("successful key setups recorded = %d, want 1", ok.firstLatency.count)
	}
	if ok.firstFailedLatency.count != 0 {
		t.Fatalf("a successful key setup was recorded as a failure (n=%d)", ok.firstFailedLatency.count)
	}
	ok.Close()
}
