package beanstalkd

import (
	"github.com/fortytw2/radish/broker"
	"github.com/nutrun/lentil"
)

type stalk struct {
	l *lentil.Beanstalkd
}

func (s *stalk) Close() error {
	return s.l.Quit()
}

func (s *stalk) Consumer(queue string) (broker.Consumer, error) {
	return newConsumer(s.l, queue)
}

func (s *stalk) Publisher(queue string) (broker.Publisher, error) {
	return newPublisher(s.l, queue)
}
