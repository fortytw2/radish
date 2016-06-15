package beanstalkd

import (
	"github.com/fortytw2/radish"
	"github.com/nutrun/lentil"
)

type stalk struct {
	l *lentil.Beanstalkd
}

func (s *stalk) Close() error {
	return s.l.Quit()
}

func (s *stalk) Consumer(queue string) (radish.Consumer, error) {
	return newConsumer(s.l, queue)
}

func (s *stalk) Publisher(queue string) (radish.Publisher, error) {
	return newPublisher(s.l, queue)
}
