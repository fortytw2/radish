package radish

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/fortytw2/radish/broker"
	"gopkg.in/inconshreveable/log15.v2"
)

// DefaultTimeout is the length a new worker will wait to get a task
var DefaultTimeout = time.Second

// A WorkFunc processes the data passed to a Worker
type WorkFunc func(interface{}) ([]interface{}, error)

// A Worker is a single unit of execution, working single threadedly
type Worker struct {
	q  string
	c  broker.Consumer
	p  broker.Publisher
	fn WorkFunc

	log  log15.Logger
	stop chan struct{}
}

// WorkerOpts are used to set up a new worker
type WorkerOpts struct {
	b     broker.Broker
	queue string
	fn    WorkFunc
	stop  chan struct{}
	log   log15.Logger
}

// NewWorker creates a new worker
func NewWorker(opts *WorkerOpts) (*Worker, error) {
	c, err := opts.b.Consumer(opts.queue)
	if err != nil {
		return nil, err
	}

	p, err := opts.b.Publisher(opts.queue)
	if err != nil {
		return nil, err
	}

	return &Worker{
		q:    opts.queue,
		c:    c,
		p:    p,
		fn:   opts.fn,
		stop: opts.stop,
		log:  opts.log,
	}, nil
}

// Work "starts" the given worker
func (w *Worker) Work(wg *sync.WaitGroup) {
	defer wg.Done()

	b := backoff.NewExponentialBackOff()
	for {
		select {
		case _, ok := <-w.stop:
			if !ok {
				w.p.Close()
				w.c.Close()
				return
			}
		default:
			var o []byte
			err := w.c.ConsumeTimeout(&o, DefaultTimeout)
			if err != nil {
				w.log.Error("could not consume from queue", "error", err)
				time.Sleep(b.NextBackOff())
				continue
			}

			n, err := w.fn(o)
			if err != nil {
				w.log.Warn("could not process task", "error", err)
				time.Sleep(b.NextBackOff())
			}

			if len(n) >= 0 {
				for _, iface := range n {
					w.p.Publish(iface)
				}
			}

			w.c.Ack()
		}
	}
}
