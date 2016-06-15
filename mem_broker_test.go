package radish

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func BenchmarkConsume(b *testing.B) {
	ConsumeBench(NewMemBroker(), 500, b)
}

func BenchmarkPublishParallel(b *testing.B) {
	ParallelPublishBench(NewMemBroker(), b)
}

func BenchmarkPublish(b *testing.B) {
	PublishBench(NewMemBroker(), b)
}

func TestMemBroker(t *testing.T) {
	defer leaktest.Check(t)()

	broker := NewMemBroker()

	// Try to publish
	pub, err := broker.Publisher("test")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = pub.Publish("hi")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = pub.Publish("there")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = pub.Publish("joe")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = pub.Close()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Try to consume
	cons, err := broker.Consumer("test")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	var out string
	err = cons.Consume(&out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "hi" {
		t.Fatalf("bad: %v", out)
	}
	err = cons.Ack()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = cons.ConsumeAck(&out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "there" {
		t.Fatalf("bad: %v", out)
	}

	err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "joe" {
		t.Fatalf("bad: %v", out)
	}

	// Push back
	err = cons.Nack()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Should get it back
	err = cons.ConsumeAck(&out)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "joe" {
		t.Fatalf("bad: %v", out)
	}

	// Should timeout
	err = cons.ConsumeTimeout(&out, 5*time.Millisecond)
	if err.Error() != "timed out" {
		t.Fatalf("err: %v", err)
	}
}
