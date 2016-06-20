package radish

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/go-kit/kit/log"
)

// DefaultTimeout is the length a new worker will wait to get a task
var DefaultTimeout = time.Second

// A WorkFunc processes the data passed to a Worker
type WorkFunc func([]byte) ([][]byte, error)

// A Worker is a single unit of execution, working single threadedly
type worker struct {
	q  string
	c  Consumer
	p  Publisher
	fn WorkFunc

	log log.Logger

	timeSinceWork *int64

	stop chan struct{}
}

// workerOpts are used to set up a new worker
type workerOpts struct {
	b     Broker
	queue string
	fn    WorkFunc
	tsw   *int64
	stop  chan struct{}
	log   log.Logger
}

// NewWorker creates a new worker
func newWorker(opts *workerOpts) (*worker, error) {
	c, err := opts.b.Consumer(opts.queue)
	if err != nil {
		return nil, err
	}

	p, err := opts.b.Publisher(opts.queue)
	if err != nil {
		return nil, err
	}

	return &worker{
		q:             opts.queue,
		c:             c,
		p:             p,
		timeSinceWork: opts.tsw,
		fn:            opts.fn,
		stop:          opts.stop,
		log:           opts.log,
	}, nil
}

// Work "starts" the given worker
func (w *worker) Work(wg *sync.WaitGroup) {
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
				t := b.NextBackOff()
				atomic.AddInt64(w.timeSinceWork, t.Nanoseconds())
				time.Sleep(t)
				continue
			} else {
				atomic.SwapInt64(w.timeSinceWork, 0)
			}

			n, err := w.fn(o)
			if err != nil {
				w.c.Nack()
				w.log.Log("msg", "could not process task", "error", err)
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
