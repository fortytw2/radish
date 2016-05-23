package broker

import "time"

// Broker implements a high level interface that
// can provide Consumer or Publisher
type Broker interface {
	// Close shuts down the broker
	Close() error
	// Consumer returns _a_ Consumer
	Consumer(queue string) (Consumer, error)
	// Publisher returns _a_ Publisher
	Publisher(queue string) (Publisher, error)
}

// Publisher is used to push messages into a queue
type Publisher interface {
	// Close will shutdown the publisher
	Close() error

	// Publish will send the message to the server to be consumed
	Publish(in interface{}) error
}

// Consumer is used to consume messages from a queue
type Consumer interface {
	// Consume will consume the next available message or times out waiting. The
	// message must be acknowledged with Ack() or Nack() before
	// the next call to Consume unless EnableMultiAck is true.
	Consume(out interface{}) error

	// ConsumeAck will consume the next message and acknowledge
	// that the message has been received. This prevents the message
	// from being redelivered, and no call to Ack() or Nack() is needed.
	ConsumeAck(out interface{}) error

	// ConsumeTimeout will consume the next available message. The
	// message must be acknowledged with Ack() or Nack() before
	// the next call to Consume unless EnableMultiAck is true.
	ConsumeTimeout(out interface{}, timeout time.Duration) error

	// Ack will send an acknowledgement to the server that the
	// last message returned by Consume was processed.
	Ack() error

	// Nack will send a negative acknowledgement to the server that the
	// last message returned by Consume was not processed and should be
	// redelivered. If EnableMultiAck is true, then all messages up to
	// the last consumed one will be negatively acknowledged
	Nack() error

	// Close will shutdown the Consumer. Any messages that are still
	// in flight will be Nack'ed.
	Close() error
}
