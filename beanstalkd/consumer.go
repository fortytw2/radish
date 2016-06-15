package beanstalkd

import (
	"sync"
	"time"

	"github.com/fortytw2/radish"
	"github.com/nutrun/lentil"
)

type consumer struct {
	l *lentil.Beanstalkd

	queue    string
	needsAck bool
	job      int
	sync.RWMutex
}

func newConsumer(l *lentil.Beanstalkd, queue string) (radish.Consumer, error) {
	return &consumer{
		queue: queue,
		l:     l,
	}, nil
}

func (c *consumer) Close() error {
	return nil
}

func (c *consumer) Ack() error {
	return nil
}

func (c *consumer) Nack() error {
	return nil
}

func (c *consumer) Consume(out interface{}) error {
	return nil
}

func (c *consumer) ConsumeAck(out interface{}) error {
	return nil
}

func (c *consumer) ConsumeTimeout(out interface{}, to time.Duration) error {
	return nil
}
