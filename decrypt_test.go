package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func newTestDispatcher(t *testing.T, count, poolLimit int) (*Dispatcher, []*DecryptInstance) {
	t.Helper()
	d := NewDispatcher()
	d.checkRegion = func(context.Context, string, string, bool) (bool, error) { return true, nil }
	instances := make([]*DecryptInstance, 0, count)
	for i := 0; i < count; i++ {
		server := newFakeDecryptServer(t)
		instance, err := NewDecryptInstance(&WrapperInstance{
			Id:          string(rune('a' + i)),
			Region:      "us",
			DecryptPort: server.port(),
		})
		if err != nil {
			t.Fatal(err)
		}
		instance.poolLimit = poolLimit
		instance.onCapacity = d.signalCapacity
		instances = append(instances, instance)
	}
	d.Instances = append(d.Instances, instances...)
	t.Cleanup(func() {
		for _, instance := range instances {
			instance.Close()
		}
	})
	return d, instances
}

func openHeldSessions(t *testing.T, d *Dispatcher, adamIDs []string) []*DecryptSession {
	t.Helper()
	sessions := make([]*DecryptSession, 0, len(adamIDs))
	for _, adamID := range adamIDs {
		session, err := d.OpenSession(context.Background(), adamID, "key")
		if err != nil {
			for _, opened := range sessions {
				opened.Close()
			}
			t.Fatal(err)
		}
		sessions = append(sessions, session)
	}
	t.Cleanup(func() {
		for _, session := range sessions {
			session.Close()
		}
	})
	return sessions
}

func assertEvenSplit(t *testing.T, sessions []*DecryptSession) {
	t.Helper()
	counts := make(map[string]int)
	for _, session := range sessions {
		counts[session.instance.id]++
	}
	if counts["a"] != 5 || counts["b"] != 5 {
		t.Fatalf("session split = %#v, want a:5 b:5", counts)
	}
}

func TestDispatcherBalancesSameAdamID(t *testing.T) {
	d, _ := newTestDispatcher(t, 2, 10)
	adamIDs := make([]string, 10)
	for i := range adamIDs {
		adamIDs[i] = "same-song"
	}
	assertEvenSplit(t, openHeldSessions(t, d, adamIDs))
}

func TestDispatcherBalancesDifferentAdamIDs(t *testing.T) {
	d, _ := newTestDispatcher(t, 2, 10)
	adamIDs := make([]string, 10)
	for i := range adamIDs {
		adamIDs[i] = string(rune('0' + i))
	}
	assertEvenSplit(t, openHeldSessions(t, d, adamIDs))
}

func TestDispatcherWarmContextOnlyBreaksEqualLoadTie(t *testing.T) {
	d, instances := newTestDispatcher(t, 2, 10)

	warm, err := instances[0].OpenSession(context.Background(), "warm-song", "key")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := warm.Decrypt("warm-song", "key", []byte("sample")); err != nil {
		t.Fatal(err)
	}
	warm.Close()

	tied, err := d.OpenSession(context.Background(), "warm-song", "key")
	if err != nil {
		t.Fatal(err)
	}
	if tied.instance != instances[0] {
		t.Fatal("warm context did not win an equal-load tie")
	}
	tied.Close()

	warmHeld, err := instances[0].OpenSession(context.Background(), "warm-song", "key")
	if err != nil {
		t.Fatal(err)
	}
	busy, err := instances[0].OpenSession(context.Background(), "other-song", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	warmHeld.Close()
	selected, err := d.OpenSession(context.Background(), "warm-song", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Close()
	if selected.instance != instances[1] {
		t.Fatal("warm context overrode a lower active-session count")
	}
}

func TestDispatcherRoutesAroundFullInstance(t *testing.T) {
	d, instances := newTestDispatcher(t, 2, 1)
	full, err := instances[0].OpenSession(context.Background(), "held", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()

	session, err := d.OpenSession(context.Background(), "new", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.instance != instances[1] {
		t.Fatal("request was not routed to the instance with free capacity")
	}
}

func TestDispatcherFullPoolsHonorCancellation(t *testing.T) {
	d, instances := newTestDispatcher(t, 2, 1)
	for _, instance := range instances {
		session, err := instance.OpenSession(context.Background(), "held", "key")
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := d.OpenSession(ctx, "waiting", "key")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenSession error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("cancellation took %s", elapsed)
	}
}

func TestDispatcherRegionLookupHonorsCancellation(t *testing.T) {
	d := NewDispatcher()
	d.Instances = []*DecryptInstance{{id: "test", region: "us"}}
	d.checkRegion = func(ctx context.Context, _, _ string, _ bool) (bool, error) {
		<-ctx.Done()
		return false, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := d.OpenSession(ctx, "song", "key")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("OpenSession error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("region cancellation took %s", elapsed)
	}
}

func TestDispatcherWaiterWakesOnRelease(t *testing.T) {
	d, instances := newTestDispatcher(t, 2, 1)
	held := make([]*DecryptSession, 0, len(instances))
	for _, instance := range instances {
		session, err := instance.OpenSession(context.Background(), "held", "key")
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, session)
	}

	result := make(chan *DecryptSession, 1)
	errCh := make(chan error, 1)
	go func() {
		session, err := d.OpenSession(context.Background(), "waiting", "key")
		if err != nil {
			errCh <- err
			return
		}
		result <- session
	}()
	time.Sleep(20 * time.Millisecond)
	held[0].Close()
	defer held[1].Close()
	select {
	case session := <-result:
		session.Close()
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("waiter did not wake after a connection was released")
	}
}

func TestDispatcherDialFailureRollsBackAndTriesAnotherInstance(t *testing.T) {
	d, healthy := newTestDispatcher(t, 1, 1)
	bad := &DecryptInstance{
		id:          "bad",
		region:      "us",
		connections: make(map[*decryptConn]struct{}),
		poolLimit:   1,
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("dial failed")
		},
		ioTimeout:  time.Second,
		onCapacity: d.signalCapacity,
	}
	d.Instances = []*DecryptInstance{bad, healthy[0]}

	session, err := d.OpenSession(context.Background(), "song", "key")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.instance != healthy[0] {
		t.Fatal("dispatcher did not retry the healthy instance")
	}
	bad.poolMu.Lock()
	defer bad.poolMu.Unlock()
	if bad.reserved != 0 || len(bad.connections) != 0 {
		t.Fatalf("failed dial leaked accounting: reserved=%d connections=%d", bad.reserved, len(bad.connections))
	}
}

func TestDispatcherConcurrentSessionsStayWithinPoolLimit(t *testing.T) {
	d, instances := newTestDispatcher(t, 2, 10)
	start := make(chan struct{})
	results := make(chan *DecryptSession, 20)
	errorsCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			<-start
			session, err := d.OpenSession(context.Background(), "song", "key")
			if err != nil {
				errorsCh <- err
				return
			}
			results <- session
		}()
	}
	close(start)
	sessions := make([]*DecryptSession, 0, 20)
	for len(sessions) < 20 {
		select {
		case err := <-errorsCh:
			t.Fatal(err)
		case session := <-results:
			sessions = append(sessions, session)
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d concurrent sessions acquired", len(sessions))
		}
	}
	defer func() {
		for _, session := range sessions {
			session.Close()
		}
	}()
	for _, instance := range instances {
		instance.poolMu.Lock()
		connections := len(instance.connections)
		reserved := instance.reserved
		instance.poolMu.Unlock()
		if connections+reserved > 10 {
			t.Fatalf("instance %s exceeded pool limit: %d", instance.id, connections+reserved)
		}
	}
	if len(sessions) != 20 {
		t.Fatalf("opened %d sessions, want 20", len(sessions))
	}
	counts := map[string]int{}
	for _, session := range sessions {
		counts[session.instance.id]++
	}
	if counts["a"] != 10 || counts["b"] != 10 {
		t.Fatalf("concurrent session split = %#v, want a:10 b:10", counts)
	}
}

func TestDispatcherPrewarmFailureDoesNotRegisterInstance(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	d := NewDispatcher()
	d.AddInstance(&WrapperInstance{Id: "bad", Region: "us", DecryptPort: port})
	if len(d.snapshotInstances()) != 0 {
		t.Fatal("failed prewarm registered an unusable instance")
	}
}

func TestInstanceCloseWithActiveSessionKeepsAccountingValid(t *testing.T) {
	_, instances := newTestDispatcher(t, 1, 2)
	instance := instances[0]
	session, err := instance.OpenSession(context.Background(), "song", "key")
	if err != nil {
		t.Fatal(err)
	}
	instance.Close()
	session.Close()
	session.Close()

	instance.poolMu.Lock()
	defer instance.poolMu.Unlock()
	if len(instance.connections) != 0 || len(instance.pool) != 0 || instance.reserved < 0 {
		t.Fatalf("invalid accounting after close: connections=%d pool=%d reserved=%d", len(instance.connections), len(instance.pool), instance.reserved)
	}
}

func TestDispatcherChecksEachRegionOnce(t *testing.T) {
	d, _ := newTestDispatcher(t, 2, 10)
	var mu sync.Mutex
	checks := 0
	d.checkRegion = func(context.Context, string, string, bool) (bool, error) {
		mu.Lock()
		checks++
		mu.Unlock()
		return true, nil
	}
	session, err := d.OpenSession(context.Background(), "song", "key")
	if err != nil {
		t.Fatal(err)
	}
	session.Close()
	if checks != 1 {
		t.Fatalf("region checks = %d, want 1", checks)
	}
}

func TestDispatcherRemoveDuringPrewarmDoesNotRegisterStaleInstance(t *testing.T) {
	d := NewDispatcher()
	started := make(chan struct{})
	proceed := make(chan struct{})
	created := &DecryptInstance{
		id:          "race",
		region:      "us",
		connections: make(map[*decryptConn]struct{}),
		poolLimit:   1,
	}
	d.newInstance = func(*WrapperInstance) (*DecryptInstance, error) {
		close(started)
		<-proceed
		return created, nil
	}
	done := make(chan struct{})
	go func() {
		d.AddInstance(&WrapperInstance{Id: "race", Region: "us"})
		close(done)
	}()
	<-started
	d.RemoveInstance("race")
	close(proceed)
	<-done

	if len(d.snapshotInstances()) != 0 {
		t.Fatal("instance removed during prewarm was registered afterward")
	}
	created.poolMu.Lock()
	defer created.poolMu.Unlock()
	if !created.isClosed {
		t.Fatal("stale prewarmed instance was not closed")
	}
}
