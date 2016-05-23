package beanstalkd

import (
	"github.com/fortytw2/radish/broker"
	"github.com/nutrun/lentil"
)

type publisher struct {
	l *lentil.Beanstalkd

	queue string
}

func newPublisher(l *lentil.Beanstalkd, queue string) (broker.Publisher, error) {
	return &publisher{
		queue: queue,
		l:     l,
	}, nil
}

func (p *publisher) Close() error {
	return nil
}

func (p *publisher) Publish(i interface{}) error {
	return nil
}
