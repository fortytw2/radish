package inmem

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/fortytw2/radish/broker"
)

// ibroker implements the Broker interface in-memory
type ibroker struct {
	Closed bool
	Queues map[string][]interface{}

	lock sync.RWMutex
}

// consumer implements the Consumer interface
type consumer struct {
	Broker      *ibroker
	Queue       string
	Closed      bool
	NeedAck     bool
	LastDequeue interface{}
}

// publisher implements the Publisher interface
type publisher struct {
	Broker *ibroker
	Queue  string
	Closed bool
}

// NewBroker returns a pure in-mem broker
func NewBroker() broker.Broker {
	in := &ibroker{
		Queues: make(map[string][]interface{}),
	}
	return in
}

func (i *ibroker) Close() error {
	i.Closed = true
	return nil
}

func (i *ibroker) Consumer(q string) (broker.Consumer, error) {
	c := &consumer{
		Broker: i,
		Queue:  q,
	}
	return c, nil
}

func (i *ibroker) Publisher(q string) (broker.Publisher, error) {
	p := &publisher{
		Broker: i,
		Queue:  q,
	}
	return p, nil
}

func (i *publisher) Close() error {
	i.Closed = true
	return nil
}

func (i *publisher) Publish(in interface{}) error {
	i.Broker.lock.Lock()
	defer i.Broker.lock.Unlock()

	queue := i.Broker.Queues[i.Queue]
	queue = append(queue, in)
	i.Broker.Queues[i.Queue] = queue
	return nil
}

func (i *consumer) Close() error {
	if i.NeedAck {
		i.Nack()
	}
	i.Closed = true
	return nil
}

func (i *consumer) Consume(out interface{}) error {
	return i.ConsumeTimeout(out, 0)
}

func (i *consumer) ConsumeAck(out interface{}) error {
	err := i.ConsumeTimeout(out, 0)
	if err == nil {
		return i.Ack()
	}
	return err
}

func (i *consumer) ConsumeTimeout(out interface{}, timeout time.Duration) error {
	if i.NeedAck {
		return errors.New("cannot consume when need ack")
	}
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	haveMsg := false
	var msg interface{}
	for {
		i.Broker.lock.Lock()
		queue := i.Broker.Queues[i.Queue]
		if len(queue) > 0 {
			msg = queue[0]
			haveMsg = true
			copy(queue[0:], queue[1:])
			i.Broker.Queues[i.Queue] = queue[:len(queue)-1]
		}
		i.Broker.lock.Unlock()
		if haveMsg {
			break
		}

		select {
		case <-time.After(time.Millisecond):
			continue
		case <-timeoutCh:
			return errors.New("timed out")
		}
	}

	// Set that we need ack
	i.NeedAck = true
	i.LastDequeue = msg

	// Set the message
	dst := reflect.Indirect(reflect.ValueOf(out))
	src := reflect.Indirect(reflect.ValueOf(msg))
	dst.Set(src)
	return nil
}

func (i *consumer) Ack() error {
	if !i.NeedAck {
		return errors.New("ack not needed")
	}
	i.NeedAck = false
	i.LastDequeue = nil
	return nil
}

func (i *consumer) Nack() error {
	if !i.NeedAck {
		return errors.New("nack not needed")
	}
	i.Broker.lock.Lock()
	defer i.Broker.lock.Unlock()

	// Push last entry back
	queue := i.Broker.Queues[i.Queue]
	n := len(queue)
	queue = append(queue, nil)
	copy(queue[1:], queue[0:n])
	queue[0] = i.LastDequeue
	i.Broker.Queues[i.Queue] = queue

	i.NeedAck = false
	i.LastDequeue = nil
	return nil
}
