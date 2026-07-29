package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var WMDispatcher *Dispatcher

type regionAvailabilityFunc func(context.Context, string, string, bool) (bool, error)

type Dispatcher struct {
	Instances   []*DecryptInstance
	mu          sync.RWMutex
	generation  map[string]uint64
	newInstance func(*WrapperInstance) (*DecryptInstance, error)
	// pendingReplacements maps the id of a condemned instance to the moment it
	// was condemned. It exists so a second condemnation cannot take the pool to
	// zero while the first wrapper is still coming back: a replacement was
	// observed taking 72s on 2026-07-29, and the backend rejects every
	// submission outright for as long as Status reports no instances.
	//
	// It is keyed by id, and the entry is cleared by the replacement for that
	// same id registering, because a wrapper keeps its id across a restart. It
	// stores a timestamp rather than a count because a replacement that never
	// arrives must stop holding the gate. See canCondemn.
	pendingReplacements map[string]time.Time

	selectionMu sync.Mutex
	roundRobin  uint64

	notifyMu   sync.Mutex
	capacityCh chan struct{}

	checkRegion regionAvailabilityFunc
	now         func() time.Time
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		Instances:           make([]*DecryptInstance, 0),
		generation:          make(map[string]uint64),
		newInstance:         NewDecryptInstance,
		pendingReplacements: make(map[string]time.Time),
		capacityCh:          make(chan struct{}),
		checkRegion:         checkAvailableOnRegionContext,
		now:                 time.Now,
	}
}

func (d *Dispatcher) clock() time.Time {
	if d.now == nil {
		return time.Now()
	}
	return d.now()
}

func (d *Dispatcher) signalCapacity() {
	d.notifyMu.Lock()
	close(d.capacityCh)
	d.capacityCh = make(chan struct{})
	d.notifyMu.Unlock()
}

func (d *Dispatcher) capacitySignal() <-chan struct{} {
	d.notifyMu.Lock()
	defer d.notifyMu.Unlock()
	return d.capacityCh
}

func (d *Dispatcher) AddInstance(inst *WrapperInstance) {
	d.mu.Lock()
	d.generation[inst.Id]++
	generation := d.generation[inst.Id]
	d.mu.Unlock()

	// Pre-warming performs wrapper I/O and must not block dispatch or lifecycle
	// operations for existing instances.
	decryptInstance, err := d.newInstance(inst)
	if err != nil {
		logrus.Errorf("failed to add instance %s: %s", inst.Id, err)
		return
	}
	decryptInstance.onCapacity = d.signalCapacity
	decryptInstance.onUnavailable = d.quarantineInstance
	decryptInstance.canCondemn = func() bool { return d.canCondemn(decryptInstance) }

	var replaced *DecryptInstance
	d.mu.Lock()
	if d.generation[inst.Id] != generation {
		d.mu.Unlock()
		decryptInstance.Close()
		return
	}
	for i, current := range d.Instances {
		if current != nil && current.id == inst.Id {
			replaced = current
			d.Instances = append(d.Instances[:i], d.Instances[i+1:]...)
			break
		}
	}
	d.Instances = append(d.Instances, decryptInstance)
	delete(d.pendingReplacements, inst.Id)
	d.prunePendingReplacementsLocked()
	d.mu.Unlock()
	if replaced != nil {
		replaced.Close()
	}
	d.signalCapacity()
	logrus.Debugf("added instance %s", inst.Id)
}

func (d *Dispatcher) quarantineInstance(target *DecryptInstance, _ string) {
	if target == nil {
		return
	}
	removed := false
	d.mu.Lock()
	for i, current := range d.Instances {
		if current == target {
			d.generation[target.id]++
			d.Instances = append(d.Instances[:i], d.Instances[i+1:]...)
			d.pendingReplacements[target.id] = d.clock()
			d.prunePendingReplacementsLocked()
			removed = true
			break
		}
	}
	d.mu.Unlock()
	if removed {
		d.signalCapacity()
	}
}

// ReplacementFailed reports that no replacement is coming for a condemned
// instance — its wrapper crash-looped, or it is not restartable at all. It is
// the fast path out of the hold in canCondemn: without it the gate would stay
// shut until pendingReplacementGrace expires, which is pure waiting for an
// answer the supervisor already knows.
func (d *Dispatcher) ReplacementFailed(id string, reason string) {
	d.mu.Lock()
	_, waiting := d.pendingReplacements[id]
	delete(d.pendingReplacements, id)
	d.prunePendingReplacementsLocked()
	d.mu.Unlock()
	if !waiting {
		return
	}
	logrus.Warnf("no replacement is coming for wrapper instance %s (%s); it no longer holds back condemning the instances that are left", id, reason)
	d.signalCapacity()
}

func (d *Dispatcher) prunePendingReplacementsLocked() {
	cutoff := d.clock().Add(-pendingReplacementGrace)
	for id, since := range d.pendingReplacements {
		if !since.After(cutoff) {
			delete(d.pendingReplacements, id)
		}
	}
}

// pendingReplacementGrace bounds how long a condemned instance's replacement is
// assumed to still be on its way, and so how long the hold in canCondemn can
// last on a deadline alone.
//
// The slowest replacement measured is 72s (2026-07-29), so the bound has to
// clear that; 120s is 1.7x it. It does not need more headroom than that,
// because the supervisor calls ReplacementFailed the moment it gives up
// restarting a wrapper, which releases the hold without waiting for the
// deadline — the deadline only covers a replacement that is neither arriving
// nor known to have failed. And erring short is the safer direction: the
// instance being held is by definition already unhealthy, so while the hold
// lasts the pool is not really serving anyway, and condemning it starts the
// restart that is the only way back. Waiting decrypts are covered separately by
// emptyPoolGrace.
const pendingReplacementGrace = 120 * time.Second

// canCondemn answers whether an unhealthy instance may be taken out of service
// now. It may not, if doing so would empty the pool while a previous
// condemnation's replacement is still plausibly starting.
//
// The alternative is what happened on 2026-07-29: two instances degraded 63s
// apart, each was condemned on the evidence of its own failures, and for the
// nine seconds between the second condemnation and the first replacement
// registering there were no instances at all. The backend checks decryptor
// status before it accepts a submission, so that window did not merely slow
// downloads down — it failed 13 jobs outright with "decryptor is not ready",
// before any track was attempted.
//
// Keeping a known-bad instance in service is the lesser harm: a decrypt it
// fails is failed over to another instance, and this one is condemned on its
// next failure once a replacement has arrived. The one case that must still go
// through is a pool with nothing pending — there, the unhealthy instance is the
// only path forward and restarting it is the whole point.
//
// "Plausibly" is the word this gate was missing. Its first version assumed a
// replacement always arrives, and on the very next incident one did not: the
// replacement wrapper died on startup four times in 52 seconds, so the pending
// slot never cleared, and a wedged instance serving nothing was held in service
// indefinitely. A hold is therefore only honoured while the replacement is
// either recent (pendingReplacementGrace) or not yet known to have failed
// (ReplacementFailed). Protecting capacity that does not work is worse than
// briefly having none.
func (d *Dispatcher) canCondemn(target *DecryptInstance) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, current := range d.Instances {
		if current != nil && current != target {
			return true
		}
	}
	// target is the last instance in service, so this condemnation empties the
	// pool. Allow it unless a replacement could still be on its way.
	cutoff := d.clock().Add(-pendingReplacementGrace)
	for _, since := range d.pendingReplacements {
		if since.After(cutoff) {
			return false
		}
	}
	return true
}

func (d *Dispatcher) RemoveInstance(id string) {
	var removed *DecryptInstance
	d.mu.Lock()
	d.generation[id]++
	for i, inst := range d.Instances {
		if inst != nil && inst.id == id {
			removed = inst
			d.Instances = append(d.Instances[:i], d.Instances[i+1:]...)
			break
		}
	}
	d.mu.Unlock()
	if removed != nil {
		removed.Close()
		d.signalCapacity()
	}
}

func (d *Dispatcher) snapshotInstances() []*DecryptInstance {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]*DecryptInstance(nil), d.Instances...)
}

func (d *Dispatcher) availableInstances(ctx context.Context, adamId string, instances []*DecryptInstance) ([]*DecryptInstance, error) {
	availability := make(map[string]bool)
	checked := make(map[string]bool)
	var firstErr error
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		if checked[inst.region] {
			continue
		}
		checked[inst.region] = true
		ok, err := d.checkRegion(ctx, adamId, inst.region, false)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		availability[inst.region] = ok
	}

	result := make([]*DecryptInstance, 0, len(instances))
	for _, inst := range instances {
		if inst != nil && availability[inst.region] {
			result = append(result, inst)
		}
	}
	if len(result) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return result, nil
}

func filterExcluded(instances []*DecryptInstance, exclude map[*DecryptInstance]bool) []*DecryptInstance {
	if len(exclude) == 0 {
		return instances
	}
	kept := make([]*DecryptInstance, 0, len(instances))
	for _, inst := range instances {
		if !exclude[inst] {
			kept = append(kept, inst)
		}
	}
	return kept
}

type instanceCandidate struct {
	instance *DecryptInstance
	load     instanceLoad
	tieOrder uint64
}

func (d *Dispatcher) reserveBest(instances []*DecryptInstance, adamId, key string, skipped map[*DecryptInstance]bool) (*DecryptInstance, *decryptConn, bool) {
	d.selectionMu.Lock()
	defer d.selectionMu.Unlock()

	candidates := make([]instanceCandidate, 0, len(instances))
	start := d.roundRobin
	for i, inst := range instances {
		if skipped[inst] {
			continue
		}
		load := inst.snapshotLoad(adamId, key)
		if !load.hasCapacity {
			continue
		}
		candidates = append(candidates, instanceCandidate{
			instance: inst,
			load:     load,
			tieOrder: (uint64(i) + uint64(len(instances)) - start%uint64(len(instances))) % uint64(len(instances)),
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].load.inUse != candidates[j].load.inUse {
			return candidates[i].load.inUse < candidates[j].load.inUse
		}
		if candidates[i].load.contextHit != candidates[j].load.contextHit {
			return candidates[i].load.contextHit
		}
		return candidates[i].tieOrder < candidates[j].tieOrder
	})

	for _, candidate := range candidates {
		conn, needsDial, ok := candidate.instance.reserveConn(adamId, key)
		if ok {
			d.roundRobin++
			return candidate.instance, conn, needsDial
		}
	}
	return nil, nil, false
}

func (d *Dispatcher) OpenSession(ctx context.Context, adamId, key string) (*DecryptSession, error) {
	return d.openSession(ctx, adamId, key, nil)
}

// OpenSessionExcluding places a session on any instance outside exclude. It is
// the failover entry point: a caller whose decrypt just faulted uses it to move
// the same work elsewhere instead of failing the client's stream. Excluding
// rather than re-ranking matters because a faulting instance sheds its sessions
// and so looks *least* loaded to reserveBest — plain re-selection would steer
// the retry straight back into it.
//
// It returns an error rather than waiting when every remaining instance is
// excluded, so the caller can report the original fault instead of hanging.
func (d *Dispatcher) OpenSessionExcluding(ctx context.Context, adamId, key string, exclude map[*DecryptInstance]bool) (*DecryptSession, error) {
	return d.openSession(ctx, adamId, key, exclude)
}

func (d *Dispatcher) openSession(ctx context.Context, adamId, key string, exclude map[*DecryptInstance]bool) (*DecryptSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		// Capture the notification before checking capacity so a release between
		// the check and wait cannot be missed.
		capacityCh := d.capacitySignal()
		instances := d.snapshotInstances()
		if len(instances) == 0 {
			// Every instance is restarting. The pool refills in seconds and
			// AddInstance signals capacity, so wait for one rather than failing
			// a decrypt that would have succeeded moments later. Bounded, so a
			// manager with no wrappers at all still answers instead of hanging.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-capacityCh:
				continue
			case <-time.After(emptyPoolGrace):
				return nil, errors.New("no available instance")
			}
		}
		available, err := d.availableInstances(ctx, adamId, instances)
		if err != nil {
			return nil, err
		}
		available = filterExcluded(available, exclude)
		if len(available) == 0 {
			return nil, fmt.Errorf("no available instance")
		}

		skipped := make(map[*DecryptInstance]bool, len(available))
		var lastDialErr error
		for len(skipped) < len(available) {
			inst, conn, needsDial := d.reserveBest(available, adamId, key, skipped)
			if inst == nil {
				break
			}
			session, openErr := inst.openReserved(ctx, conn, needsDial)
			if openErr == nil {
				return session, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastDialErr = openErr
			skipped[inst] = true
		}
		if lastDialErr != nil && len(skipped) == len(available) {
			return nil, lastDialErr
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-capacityCh:
		}
	}
}
