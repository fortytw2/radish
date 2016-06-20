package radish

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
)

// A Pool is a set of workers that all function on the same queue
type Pool struct {
	queue string
	b     Broker

	fn WorkFunc

	tsw int64
	log log.Logger
	// workers are kept track of by their stop channel
	workers []chan struct{}
	wg      *sync.WaitGroup
	*sync.RWMutex
}

// NewPool returns a configurable pool
func NewPool(b Broker, queue string, fn WorkFunc, l log.Logger) *Pool {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &Pool{
		queue: queue,
		b:     b,

		fn: fn,

		workers: make([]chan struct{}, 0),
		wg:      &sync.WaitGroup{},
		RWMutex: &sync.RWMutex{},
		log:     l,
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

func (p *Pool) TotalTimeSinceWork() time.Duration {
	return time.Duration(atomic.LoadInt64(&p.tsw))
}

func (p *Pool) addWorker() error {
	stop := make(chan struct{})
	p.wg.Add(1)
	w, err := newWorker(&workerOpts{
		b:     p.b,
		queue: p.queue,
		fn:    p.fn,
		tsw:   &p.tsw,
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
