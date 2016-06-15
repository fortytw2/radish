package radish

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestPool(t *testing.T) {
	defer leaktest.Check(t)()

	q := RandomQueue()
	b := testBroker(t)

	var total int64
	p := NewPool(b, q, func(i interface{}) ([]interface{}, error) {
		atomic.AddInt64(&total, 1)
		return nil, nil
	}, nil)

	pub, err := b.Publisher(q)
	if err != nil {
		t.Fatal("could not get a publisher", err)
	}

	for i := 0; i < 20; i++ {
		err = pub.Publish([]byte("message!"))
		if err != nil {
			t.Fatal("could not publish message", err)
		}
	}

	err = p.AddWorkers(20)
	if err != nil {
		t.Fatal("could not add workers", err)
	}

	time.Sleep(time.Second)
	err = p.Stop()
	if err != nil {
		t.Fatal("could not stop pool", err)
	}

	if total != 20 {
		t.Fatalf("did not work all 20 tasks, instead %d", total)
	}
}

func TestConcurrentPool(t *testing.T) {
	defer leaktest.Check(t)()

	q := RandomQueue()
	q2 := RandomQueue()
	b := testBroker(t)

	var total int64
	p := NewPool(b, q, func(i interface{}) ([]interface{}, error) {
		atomic.AddInt64(&total, 1)
		return nil, nil
	}, nil)

	p2 := NewPool(b, q2, func(i interface{}) ([]interface{}, error) {
		atomic.AddInt64(&total, 1)
		return nil, nil
	}, nil)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		pub, err := b.Publisher(q)
		if err != nil {
			t.Fatal("could not get a publisher", err)
		}

		for i := 0; i < 20; i++ {
			err = pub.Publish([]byte("message!"))
			if err != nil {
				t.Fatal("could not publish message", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		pub2, err := b.Publisher(q2)
		if err != nil {
			t.Fatal("could not get a publisher", err)
		}

		for i := 0; i < 20; i++ {
			err = pub2.Publish([]byte("message!"))
			if err != nil {
				t.Fatal("could not publish message", err)
			}
		}
	}()
	wg.Wait()

	err := p.AddWorkers(20)
	if err != nil {
		t.Fatal("could not add workers", err)
	}

	err = p2.AddWorkers(20)
	if err != nil {
		t.Fatal("could not add workers", err)
	}

	time.Sleep(time.Second)
	err = p.Stop()
	if err != nil {
		t.Fatal("could not stop pool", err)
	}

	err = p2.Stop()
	if err != nil {
		t.Fatal("could not stop pool", err)
	}

	if total != 40 {
		t.Fatalf("did not work all 40 tasks, instead %d", total)
	}
}

func testBroker(t *testing.T) Broker {
	return NewMemBroker()
}
