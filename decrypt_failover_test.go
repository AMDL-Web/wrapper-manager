package main

import (
	"context"
	"testing"

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
