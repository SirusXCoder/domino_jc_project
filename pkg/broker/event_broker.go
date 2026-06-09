package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Event is a topic-addressed message delivered asynchronously to subscribers.
type Event struct {
	Topic     string
	Payload   interface{}
	Timestamp int64
}

// Broker publishes events to topics and exposes per-topic subscription streams.
type Broker interface {
	Publish(ctx context.Context, event Event) error
	Subscribe(ctx context.Context, topic string) (<-chan Event, error)
}

const defaultSubscriberBuffer = 64

// ErrEmptyTopic is returned when Publish or Subscribe is called without a topic.
var ErrEmptyTopic = errors.New("broker: topic is required")

type subscriber struct {
	ch chan Event
}

// InMemoryBroker is a thread-safe, channel-backed broker for single-process use.
type InMemoryBroker struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscriber
	bufferSize  int
	retention   *DroppedEventRetentionBuffer
	dropLogger  DroppedEventsLogger
	closeOnce   sync.Once
	closed      bool
}

// InMemoryBrokerOption configures optional InMemoryBroker behavior.
type InMemoryBrokerOption func(*InMemoryBroker)

// WithSubscriberBuffer sets the per-subscriber channel capacity.
func WithSubscriberBuffer(size int) InMemoryBrokerOption {
	return func(b *InMemoryBroker) {
		if size > 0 {
			b.bufferSize = size
		}
	}
}

// WithDroppedEventRetention attaches a retention buffer for dropped-event audit trails.
func WithDroppedEventRetention(buf *DroppedEventRetentionBuffer) InMemoryBrokerOption {
	return func(b *InMemoryBroker) {
		b.retention = buf
	}
}

// WithDroppedEventsLogger registers a callback invoked for every dropped event.
func WithDroppedEventsLogger(logger DroppedEventsLogger) InMemoryBrokerOption {
	return func(b *InMemoryBroker) {
		b.dropLogger = logger
	}
}

// NewInMemoryBroker constructs an empty in-memory event broker.
func NewInMemoryBroker(opts ...InMemoryBrokerOption) *InMemoryBroker {
	b := &InMemoryBroker{
		subscribers: make(map[string][]*subscriber),
		bufferSize:  defaultSubscriberBuffer,
		retention:   NewDroppedEventRetentionBuffer(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// DroppedRetentionRecords returns a snapshot of all retained drop audit entries.
func (b *InMemoryBroker) DroppedRetentionRecords() []DroppedEventRecord {
	if b == nil || b.retention == nil {
		return nil
	}
	return b.retention.Records()
}

// Close shuts down the broker and closes all subscriber channels.
// It is safe to call multiple times.
func (b *InMemoryBroker) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		for topic, subs := range b.subscribers {
			for _, sub := range subs {
				close(sub.ch)
			}
			delete(b.subscribers, topic)
		}
		b.mu.Unlock()
	})
}

// Publish delivers an event to every active subscriber on the event's topic.
func (b *InMemoryBroker) Publish(ctx context.Context, event Event) error {
	if event.Topic == "" {
		return ErrEmptyTopic
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return errors.New("broker: closed")
	}
	b.mu.RUnlock()
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}

	b.mu.RLock()
	subs := b.subscribers[event.Topic]
	targets := make([]*subscriber, len(subs))
	copy(targets, subs)
	b.mu.RUnlock()

	for _, sub := range targets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sub.ch <- event:
		default:
			b.logDroppedEvent(event, DropReasonSubscriberBufferFull)
		}
	}
	return nil
}

func (b *InMemoryBroker) logDroppedEvent(event Event, reason DropReason) {
	droppedAt := time.Now()
	record := DroppedEventRecord{
		Event:             event,
		Reason:            reason,
		DroppedAt:         droppedAt,
		DroppedAtUnixNano: droppedAt.UnixNano(),
	}
	if b.retention != nil {
		b.retention.Append(record)
	}
	if b.dropLogger != nil {
		b.dropLogger(record)
	}
}

// Subscribe registers a buffered receive channel for the given topic.
// The channel is closed when ctx is cancelled.
func (b *InMemoryBroker) Subscribe(ctx context.Context, topic string) (<-chan Event, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}

	sub := &subscriber{ch: make(chan Event, b.bufferSize)}

	b.mu.Lock()
	b.subscribers[topic] = append(b.subscribers[topic], sub)
	b.mu.Unlock()

	go func() {
		<-ctx.Done()
		b.removeSubscriber(topic, sub)
		close(sub.ch)
	}()

	return sub.ch, nil
}

func (b *InMemoryBroker) removeSubscriber(topic string, sub *subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs := b.subscribers[topic]
	for i, candidate := range subs {
		if candidate == sub {
			b.subscribers[topic] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(b.subscribers[topic]) == 0 {
		delete(b.subscribers, topic)
	}
}
