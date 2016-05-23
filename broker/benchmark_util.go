package broker

import "testing"

// PublishBench benchmarks publisher.Publish
func PublishBench(br Broker, b *testing.B) {
	b.ReportAllocs()

	p, err := br.Publisher("benchpub")
	if err != nil {
		b.Fatalf("could not get publisher, %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Publish("test payload")
	}
}

// ParallelPublishBench benchmarks publisher.Publish in parallel
func ParallelPublishBench(br Broker, b *testing.B) {
	p, err := br.Publisher("benchpub")
	if err != nil {
		b.Fatalf("could not get publisher, %s", err)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Publish("test payload")
		}
	})
}

func ConsumeBench(br Broker, n int, b *testing.B) {
	p, err := br.Publisher("benchconsume")
	if err != nil {
		b.Fatalf("could not get publisher, %s", err)
	}

	c, err := br.Consumer("benchconsume")
	if err != nil {
		b.Fatalf("could not get consumer, %s", err)
	}

	b.ResetTimer()

	b.SetBytes(int64(len([]byte("test payload"))))
	for i := 0; i < b.N; i++ {
		err := p.Publish("test payload")
		if err != nil {
			b.Fatal(err)
		}

		var s string
		err = c.Consume(&s)
		if err != nil {
			b.Fatal(err)
		}

		err = c.Ack()
		if err != nil {
			b.Fatal(err)
		}
	}
}
