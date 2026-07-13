package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"sync"
)

var WMDispatcher *Dispatcher

type Dispatcher struct {
	Instances []*DecryptInstance
	mu        sync.RWMutex
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		Instances: make([]*DecryptInstance, 0),
		mu:        sync.RWMutex{},
	}
}

func (d *Dispatcher) AddInstance(inst *WrapperInstance) {
	d.mu.Lock()
	defer d.mu.Unlock()
	decryptInstance, err := NewDecryptInstance(inst)
	if err != nil {
		logrus.Errorf("failed to add instance %s: %s", inst.Id, err)
	}
	d.Instances = append(d.Instances, decryptInstance)
	logrus.Debugf("added instance %s", inst.Id)
}

func (d *Dispatcher) RemoveInstance(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.Instances) == 0 {
		return
	}
	for i, inst := range d.Instances {
		if inst == nil {
			continue
		}
		if inst.id == id {
			d.Instances = append(d.Instances[:i], d.Instances[i+1:]...)
			break
		}
	}
}

func (d *Dispatcher) OpenSession(adamId, key string) (*DecryptSession, error) {
	inst := d.selectInstance(adamId)
	if inst == nil {
		return nil, fmt.Errorf("no available instance")
	}
	return inst.OpenSession(adamId, key)
}

func (d *Dispatcher) selectInstance(adamId string) *DecryptInstance {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.Instances) == 0 {
		return nil
	}

	for _, inst := range d.Instances {
		if inst.GetLastAdamId() == adamId {
			// logrus.Debugf("selected instance %s for adamid %s, method 1", inst.id, adamId)
			return inst
		}
	}

	for _, inst := range d.Instances {
		ok, _ := checkAvailableOnRegion(adamId, inst.region, false)
		if inst.GetLastAdamId() == "" && ok {
			// logrus.Debugf("selected instance %s for adamid %s, method 2", inst.id, adamId)
			return inst
		}
	}

	var candidates []*DecryptInstance

	for _, inst := range d.Instances {
		ok, _ := checkAvailableOnRegion(adamId, inst.region, false)
		if ok {
			candidates = append(candidates, inst)
		}
	}

	if len(candidates) > 0 {
		return candidates[rand.Intn(len(candidates))]
	}

	return nil
}
