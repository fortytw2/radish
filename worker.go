package radish

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
)

// DefaultTimeout is the length a new worker will wait to get a task
var DefaultTimeout = time.Second

// A WorkFunc processes the data passed to a Worker
type WorkFunc func(i interface{}) error

// A Worker is a single unit of execution, working single threadedly
type worker struct {
	q  string
	c  Consumer
	p  Publisher
	fn WorkFunc

	log log.Logger

	timeSinceWork *int64
	workLock      *int64

	stop chan struct{}
}

// workerOpts are used to set up a new worker
type workerOpts struct {
	b        Broker
	queue    string
	fn       WorkFunc
	tsw      *int64
	workLock *int64
	stop     chan struct{}
	log      log.Logger
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
		workLock:      opts.workLock,
		fn:            opts.fn,
		stop:          opts.stop,
		log:           opts.log,
	}, nil
}

// Work "starts" the given worker
func (w *worker) Work(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case _, ok := <-w.stop:
			if !ok {
				w.p.Close()
				w.c.Close()
				return
			}
		default:
			var o interface{}
			err := w.c.ConsumeTimeout(&o, DefaultTimeout)
			if err != nil {
				one := atomic.LoadInt64(w.workLock)
				if one == 0 {
					atomic.AddInt64(w.timeSinceWork, int64(DefaultTimeout))
				}
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				atomic.SwapInt64(w.timeSinceWork, 0)
			}

			atomic.AddInt64(w.workLock, 1)
			err = w.fn(o)
			if err != nil {
				w.c.Ack()
				continue
			}
			atomic.AddInt64(w.workLock, -1)

			w.c.Ack()
		}
	}
}
