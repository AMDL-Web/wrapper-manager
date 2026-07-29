package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/AMDL-Web/wrapper-manager/proto"
)

// pinnedSession opens a session directly on one instance, bypassing dispatcher
// selection, so a test can decide which instance faults first.
func pinnedSession(t *testing.T, instance *DecryptInstance, adamID, key string) *DecryptSession {
	t.Helper()
	session, err := instance.OpenSession(context.Background(), adamID, key)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestDecryptFailoverMovesSampleToHealthyInstance(t *testing.T) {
	d, instances, servers := newTestDispatcherWithServers(t, 2, maxPoolSize)
	servers[0].faulty.Store(true)

	session := pinnedSession(t, instances[0], "1", "key")
	data := &pb.DecryptData{AdamId: "1", Key: "key", Sample: []byte("transform")}

	result, next, err := decryptWithFailover(context.Background(), d, session, data)
	if err != nil {
		t.Fatalf("decryptWithFailover returned %v, want the healthy instance to serve the sample", err)
	}
	if next == nil {
		t.Fatal("decryptWithFailover returned a nil session after succeeding")
	}
	defer next.Close()
	if next.instance != instances[1] {
		t.Fatalf("session landed on instance %s, want the healthy instance %s", next.instance.id, instances[1].id)
	}

	// "transform" is XOR 0xFF'd by the fake wrapper. Getting it back proves the
	// healthy instance received the original ciphertext, not a buffer the failed
	// attempt had already written plaintext into.
	want := []byte("transform")
	for i := range want {
		want[i] ^= 0xFF
	}
	if string(result) != string(want) {
		t.Fatalf("plaintext = %q, want %q", result, want)
	}
	if got := servers[1].samples.Load(); got != 1 {
		t.Fatalf("healthy instance saw %d samples, want 1", got)
	}
}

func TestDecryptFailoverStopsWhenEveryInstanceFaults(t *testing.T) {
	d, instances, servers := newTestDispatcherWithServers(t, 2, maxPoolSize)
	servers[0].faulty.Store(true)
	servers[1].faulty.Store(true)

	session := pinnedSession(t, instances[0], "1", "key")
	data := &pb.DecryptData{AdamId: "1", Key: "key", Sample: []byte("sample")}

	_, next, err := decryptWithFailover(context.Background(), d, session, data)
	if err == nil {
		t.Fatal("decryptWithFailover succeeded with every instance faulting")
	}
	if next != nil {
		next.Close()
		t.Fatal("decryptWithFailover returned a session after exhausting every instance")
	}
	// Each instance is tried once for this sample; neither is retried.
	if got := servers[0].samples.Load(); got != 1 {
		t.Fatalf("instance a saw %d samples, want 1", got)
	}
	if got := servers[1].samples.Load(); got != 1 {
		t.Fatalf("instance b saw %d samples, want 1", got)
	}
}

func TestDecryptFailoverSkipsSingleInstanceDeployment(t *testing.T) {
	d, instances, servers := newTestDispatcherWithServers(t, 1, maxPoolSize)
	servers[0].faulty.Store(true)

	session := pinnedSession(t, instances[0], "1", "key")
	data := &pb.DecryptData{AdamId: "1", Key: "key", Sample: []byte("sample")}

	_, next, err := decryptWithFailover(context.Background(), d, session, data)
	if err == nil {
		t.Fatal("decryptWithFailover succeeded with the only instance faulting")
	}
	if next != nil {
		next.Close()
		t.Fatal("decryptWithFailover returned a session with no instance left to use")
	}
	if got := servers[0].samples.Load(); got != 1 {
		t.Fatalf("sole instance saw %d samples, want 1 (no pointless retry)", got)
	}
}

// A read that already delivered bytes has overwritten part of the request
// buffer, because decryption reads plaintext back over the ciphertext. Replaying
// that buffer elsewhere would produce silently wrong audio, so the fault must
// not be treated as replayable.
func TestDecryptPartialReadDoesNotFailOver(t *testing.T) {
	d, instances, servers := newTestDispatcherWithServers(t, 2, maxPoolSize)

	session := pinnedSession(t, instances[0], "1", "key")
	data := &pb.DecryptData{AdamId: "1", Key: "key", Sample: []byte("partial")}

	_, next, err := decryptWithFailover(context.Background(), d, session, data)
	if err == nil {
		t.Fatal("decryptWithFailover succeeded on a partially read sample")
	}
	if next != nil {
		next.Close()
		t.Fatal("decryptWithFailover returned a session after a partial read")
	}
	if got := servers[1].samples.Load(); got != 0 {
		t.Fatalf("healthy instance saw %d samples, want 0: a corrupt buffer must not be replayed", got)
	}
}

// A cancelled client is not evidence that the wrapper is unhealthy, and the
// client is no longer waiting for the sample. Spending another instance's
// capacity on it would be pure waste.
func TestDecryptCancellationDoesNotFailOver(t *testing.T) {
	d, instances, servers := newTestDispatcherWithServers(t, 2, maxPoolSize)

	ctx, cancel := context.WithCancel(context.Background())
	session, err := instances[0].OpenSession(ctx, "1", "key")
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	data := &pb.DecryptData{AdamId: "1", Key: "key", Sample: []byte("sample")}
	_, next, err := decryptWithFailover(ctx, d, session, data)
	if err == nil {
		t.Fatal("decryptWithFailover succeeded on a cancelled stream")
	}
	if next != nil {
		next.Close()
		t.Fatal("decryptWithFailover returned a session for a cancelled stream")
	}
	if got := servers[1].samples.Load(); got != 0 {
		t.Fatalf("healthy instance saw %d samples, want 0 on client cancellation", got)
	}
}

func TestOpenSessionExcludingSkipsNamedInstance(t *testing.T) {
	d, instances, _ := newTestDispatcherWithServers(t, 2, maxPoolSize)

	for i := 0; i < 6; i++ {
		session, err := d.OpenSessionExcluding(context.Background(), "1", "key", map[*DecryptInstance]bool{instances[0]: true})
		if err != nil {
			t.Fatal(err)
		}
		if session.instance != instances[1] {
			t.Fatalf("session %d landed on excluded instance %s", i, session.instance.id)
		}
		defer session.Close()
	}
}

func TestOpenSessionExcludingFailsWhenNothingRemains(t *testing.T) {
	d, instances, _ := newTestDispatcherWithServers(t, 2, maxPoolSize)

	exclude := map[*DecryptInstance]bool{instances[0]: true, instances[1]: true}
	session, err := d.OpenSessionExcluding(context.Background(), "1", "key", exclude)
	if err == nil {
		session.Close()
		t.Fatal("OpenSessionExcluding returned a session with every instance excluded")
	}
}

// The two budgets exist because the two populations differ by four orders of
// magnitude. Pin that they are actually applied per operation, not blended
// back into one effective deadline.
func TestFirstSampleAfterContextSwitchGetsTheLongerDeadline(t *testing.T) {
	newInstance := func(id string) *DecryptInstance {
		t.Helper()
		server := newFakeDecryptServer(t)
		instance, err := NewDecryptInstance(&WrapperInstance{Id: id, Region: "cn", DecryptPort: server.port()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(instance.Close)
		instance.ioTimeout = 50 * time.Millisecond
		instance.firstSampleTimeout = 1500 * time.Millisecond
		instance.terminateWrapper = func() error { return nil }
		return instance
	}

	// Separate instances so neither sees two timeouts and quarantines itself
	// mid-test.
	first := newInstance("first")
	steady := newInstance("steady")

	// A session's opening decrypt always switches context away from the
	// pre-warm key, so this one is charged the long budget.
	firstSession, err := first.OpenSession(context.Background(), "song-1", "key-1")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := firstSession.Decrypt("song-1", "key-1", []byte("block")); err == nil {
		t.Fatal("expected the blocked sample to time out")
	}
	if elapsed := time.Since(started); elapsed < first.firstSampleTimeout/2 {
		t.Fatalf("first sample after a context switch timed out in %s, i.e. on the steady deadline (%s) rather than its own (%s)",
			elapsed, first.ioTimeout, first.firstSampleTimeout)
	}

	steadySession, err := steady.OpenSession(context.Background(), "song-2", "key-2")
	if err != nil {
		t.Fatal(err)
	}
	// Spend the context switch on a sample the server answers, so the next one
	// is steady state.
	if _, err := steadySession.Decrypt("song-2", "key-2", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	if _, err := steadySession.Decrypt("song-2", "key-2", []byte("block")); err == nil {
		t.Fatal("expected the blocked sample to time out")
	}
	if elapsed := time.Since(started); elapsed >= steady.firstSampleTimeout/2 {
		t.Fatalf("steady-state sample timed out in %s, i.e. on the first-sample deadline (%s) rather than its own (%s)",
			elapsed, steady.firstSampleTimeout, steady.ioTimeout)
	}
}

// The regression this pins actually shipped: a first-sample timeout was counted
// as evidence of a wedged wrapper, so an album's worth of legitimately slow key
// setups restarted healthy instances and failed whole jobs. The first decrypt
// after a context switch has no characterised ceiling — healthy instances have
// been measured past ten seconds — so exceeding its budget must cost the sample
// a failover and nothing more.
//
// Amended 2026-07-29. This test used to block the first sample FOREVER, which is
// not a slow instance — it is the wedge, and asserting that it must not be
// condemned is what let two wrappers fail over to each other for an hour with
// Status reporting ready the whole time. What the test means to protect is an
// instance that is slow and still gets there, so that is what it now models: the
// timeouts are interleaved with completions. The streak resets on any completed
// first sample, so slowness never accumulates into a verdict.
// TestEmptyFirstSampleTimeoutCondemnsTheInstance covers the other population.
func TestSlowFirstSampleDoesNotCountAgainstInstanceHealth(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(instance.Close)
	// A budget the fake server will always blow through on the opening decrypt,
	// which is charged the first-sample budget because it switches context.
	instance.ioTimeout = 30 * time.Millisecond
	instance.firstSampleTimeout = 30 * time.Millisecond
	terminated := make(chan struct{}, 4)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}

	// Far more than either health threshold, across distinct connections and
	// songs so the general failure rule would trip too if these reached it.
	// Each slow first sample is followed by one that completes, which is what
	// "slow but healthy" means and what a wedged instance can never produce.
	for i, adamID := range []string{"song-1", "song-2", "song-3", "song-4"} {
		session, err := instance.OpenSession(context.Background(), adamID, fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("instance stopped serving after %d slow first samples: %v", i, err)
		}
		if _, err := session.Decrypt(adamID, fmt.Sprintf("key-%d", i), []byte("block")); err == nil {
			t.Fatal("expected the first sample to exceed its budget")
		}
		session.Close()
		// A fresh session, so this decrypt is itself a first-after-switch — and
		// it succeeds, proving the wrapper still does key setup.
		recovered, err := instance.OpenSession(context.Background(), adamID, fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("instance stopped serving after %d slow first samples: %v", i, err)
		}
		if _, err := recovered.Decrypt(adamID, fmt.Sprintf("key-%d", i), []byte("ok")); err != nil {
			t.Fatalf("a healthy first sample failed: %v", err)
		}
		recovered.Close()
	}

	select {
	case <-terminated:
		t.Fatal("slow first samples restarted a healthy wrapper")
	case <-time.After(100 * time.Millisecond):
	}
	instance.poolMu.Lock()
	closed := instance.isClosed
	instance.poolMu.Unlock()
	if closed {
		t.Fatal("slow first samples quarantined a healthy instance")
	}
}

// The steady-state budget sits ~400x above the worst sample ever measured, so
// timeouts there stay conclusive and must still reach the wedged-wrapper rule.
func TestSteadyStateTimeoutsStillQuarantine(t *testing.T) {
	server := newFakeDecryptServer(t)
	instance, err := NewDecryptInstance(&WrapperInstance{Id: "test", Region: "cn", DecryptPort: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	instance.ioTimeout = 30 * time.Millisecond
	instance.firstSampleTimeout = 5 * time.Second
	terminated := make(chan struct{}, 2)
	instance.terminateWrapper = func() error {
		terminated <- struct{}{}
		return nil
	}

	for i := 0; i < wrapperTimeoutThreshold; i++ {
		session, err := instance.OpenSession(context.Background(), "song-1", "key-1")
		if err != nil {
			t.Fatal(err)
		}
		// First decrypt establishes the context and succeeds, so the second is
		// charged the steady-state budget and is the one that times out.
		if _, err := session.Decrypt("song-1", "key-1", []byte("ok")); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Decrypt("song-1", "key-1", []byte("block")); err == nil {
			t.Fatal("expected a steady-state timeout")
		}
	}

	select {
	case <-terminated:
	case <-time.After(2 * time.Second):
		t.Fatal("steady-state timeouts no longer quarantine a wedged wrapper")
	}
}

// TestFirstSampleWedgeIsCondemnedThroughDecrypt is the end-to-end counterpart to
// TestEmptyFirstSampleTimeoutCondemnsTheInstance. That test calls
// observeWrapperIOFailure directly and hands it the conclusive flag, so it pins
// the rule but not the wiring — with it alone, a caller that stopped deriving
// the flag from the read count would go unnoticed, which is precisely the
// regression that caused the 2026-07-29 outage. This one drives real Decrypt
// calls against a server that produces each population.
func TestFirstSampleWedgeIsCondemnedThroughDecrypt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		payload    string
		wantWedged bool
	}{
		// Nothing ever comes back. Thirty seconds of silence is not a slow key
		// setup, it is a wrapper that never answered.
		{name: "first samples return no bytes at all", payload: "block", wantWedged: true},
		// Bytes come back and then it stalls. Slow, but it is talking.
		{name: "first samples return bytes then stall", payload: "partial-block", wantWedged: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newFakeDecryptServer(t)
			instance, err := NewDecryptInstance(&WrapperInstance{Id: "wedge-e2e", Region: "cn", DecryptPort: server.port()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(instance.Close)
			instance.ioTimeout = 30 * time.Millisecond
			instance.firstSampleTimeout = 30 * time.Millisecond
			terminated := make(chan struct{}, 8)
			instance.terminateWrapper = func() error {
				terminated <- struct{}{}
				return nil
			}

			// Each iteration opens a fresh session, so every decrypt below is a
			// first-after-context-switch and is charged the first-sample budget.
			// No successful sample is interleaved: this is an unbroken streak.
			for i := 0; i < wrapperTimeoutThreshold; i++ {
				adamID := fmt.Sprintf("song-%d", i)
				session, err := instance.OpenSession(context.Background(), adamID, fmt.Sprintf("key-%d", i))
				if err != nil {
					if tc.wantWedged {
						break // already condemned, which is the point
					}
					t.Fatalf("instance stopped serving after %d slow first samples: %v", i, err)
				}
				if _, err := session.Decrypt(adamID, fmt.Sprintf("key-%d", i), []byte(tc.payload)); err == nil {
					t.Fatal("expected the first sample to exceed its budget")
				}
				session.Close()
			}

			instance.poolMu.Lock()
			wedged := instance.isClosed
			instance.poolMu.Unlock()
			if wedged != tc.wantWedged {
				t.Errorf("instance quarantined = %v after %d first-sample timeouts, want %v",
					wedged, wrapperTimeoutThreshold, tc.wantWedged)
			}
			select {
			case <-terminated:
				if !tc.wantWedged {
					t.Error("a slow but responsive wrapper was restarted")
				}
			case <-time.After(100 * time.Millisecond):
				if tc.wantWedged {
					t.Error("a wedged wrapper was never restarted")
				}
			}
		})
	}
}
