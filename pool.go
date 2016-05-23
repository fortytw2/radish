package radish

import (
	"errors"
	"sync"

	"github.com/fortytw2/radish/broker"
	"gopkg.in/inconshreveable/log15.v2"
)

// A Pool is a set of workers that all function on the same queue
type Pool struct {
	queue string
	b     broker.Broker

	fn WorkFunc

	log log15.Logger
	// workers are only kept track of by their stop channel
	workers []chan struct{}
	wg      *sync.WaitGroup
	*sync.RWMutex
}

// NewPool returns a configurable pool
func NewPool(b broker.Broker, queue string, fn WorkFunc, log log15.Logger) *Pool {
	if log == nil {
		l := log15.New()
		l.SetHandler(log15.DiscardHandler())
		log = l
	}

	return &Pool{
		queue: queue,
		b:     b,

		fn: fn,

		workers: make([]chan struct{}, 0),
		wg:      &sync.WaitGroup{},
		RWMutex: &sync.RWMutex{},
		log:     log,
	}
}

// AddWorkers changes the current number of workers in a pool
func (p *Pool) AddWorkers(n int) error {
	if n >= 1 {
		for i := 0; i < n; i++ {
			err := p.addWorker()
			if err != nil {
				return err
			}
		}
	} else if n <= -1 {
		if n*-1 > p.Len() {
			return errors.New("cannot remove more workers than exist")
		}
		for i := 0; i < (-1)*n; i++ {
			p.removeWorker()
		}
	}

	return nil
}

// Len returns the total number of workers in this group
func (p *Pool) Len() int {
	p.RLock()
	defer p.RUnlock()
	return len(p.workers)
}

// Stop turns off all workers in the pool
func (p *Pool) Stop() error {
	err := p.AddWorkers(-1 * p.Len())
	if err != nil {
		return err
	}
	p.wg.Wait()
	return nil
}

func (p *Pool) addWorker() error {
	stop := make(chan struct{})
	p.wg.Add(1)
	w, err := NewWorker(&WorkerOpts{
		b:     p.b,
		queue: p.queue,
		fn:    p.fn,
		stop:  stop,
		log:   p.log,
	})
	if err != nil {
		return err
	}

	go w.Work(p.wg)

	p.Lock()
	p.workers = append(p.workers, stop)
	p.Unlock()

	return nil
}

func (p *Pool) removeWorker() {
	p.Lock()
	// close channel to stop worker
	close(p.workers[len(p.workers)-1])

	// from github.com/golang/go/wiki/SliceTricks
	// Delete without preserving order
	p.workers[len(p.workers)-1] = nil
	p.workers = p.workers[:len(p.workers)-1]
	// unlock
	p.Unlock()
}
