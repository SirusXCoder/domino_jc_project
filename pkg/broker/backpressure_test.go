package broker_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"domino_jc_project/pkg/broker"
)

func TestBroker_BackpressureAndDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.NewInMemoryBroker()
	const (
		topic      = "test.backpressure"
		eventCount = 5000
	)

	events, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	consumeCtx, stopConsumer := context.WithCancel(ctx)
	var consumerWG sync.WaitGroup
	consumerWG.Add(1)

	var received atomic.Int64
	go func() {
		defer consumerWG.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case _, ok := <-events:
				if !ok {
					return
				}
				time.Sleep(100 * time.Millisecond)
				received.Add(1)
			}
		}
	}()

	start := time.Now()
	var publishWG sync.WaitGroup
	publishWG.Add(1)
	go func() {
		defer publishWG.Done()
		for i := 0; i < eventCount; i++ {
			if err := b.Publish(ctx, broker.Event{
				Topic:   topic,
				Payload: i,
			}); err != nil {
				t.Errorf("Publish %d: %v", i, err)
				return
			}
		}
	}()
	publishWG.Wait()
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Fatalf("publishing %d events took %v; want < 50ms so Publish never blocks on slow consumers", eventCount, elapsed)
	}

	stopConsumer()
	consumerWG.Wait()

	var pending int64
	for {
		select {
		case <-events:
			pending++
		default:
			goto countDone
		}
	}
countDone:

	delivered := received.Load()
	droppedRecords := b.DroppedRetentionRecords()
	dropped := int64(len(droppedRecords))

	if delivered >= eventCount {
		t.Fatalf("received %d events, want fewer than %d to confirm drop-on-full backpressure", delivered, eventCount)
	}
	if delivered+pending+dropped != eventCount {
		t.Fatalf("audit gap: delivered=%d pending=%d dropped=%d total=%d, want 100%% accounting across %d publishes",
			delivered, pending, dropped, delivered+pending+dropped, eventCount)
	}

	payloadSeen := make(map[int]struct{}, dropped)
	for i, record := range droppedRecords {
		if record.Event.Topic != topic {
			t.Fatalf("record[%d].topic = %q, want %q", i, record.Event.Topic, topic)
		}
		if record.Reason != broker.DropReasonSubscriberBufferFull {
			t.Fatalf("record[%d].reason = %q, want %q", i, record.Reason, broker.DropReasonSubscriberBufferFull)
		}
		if record.Event.Timestamp == 0 {
			t.Fatalf("record[%d] missing original event timestamp", i)
		}
		if record.DroppedAt.IsZero() || record.DroppedAtUnixNano == 0 {
			t.Fatalf("record[%d] missing high-resolution drop timestamp", i)
		}
		if record.DroppedAtUnixNano != record.DroppedAt.UnixNano() {
			t.Fatalf("record[%d] drop timestamp mismatch: unix_nano=%d dropped_at=%d",
				i, record.DroppedAtUnixNano, record.DroppedAt.UnixNano())
		}

		seq, ok := record.Event.Payload.(int)
		if !ok {
			t.Fatalf("record[%d].payload type = %T, want int", i, record.Event.Payload)
		}
		if _, dup := payloadSeen[seq]; dup {
			t.Fatalf("retention log contains duplicate dropped payload seq=%d", seq)
		}
		payloadSeen[seq] = struct{}{}
	}

	if int64(len(payloadSeen)) != dropped {
		t.Fatalf("retention log captured %d unique payloads, want %d dropped events", len(payloadSeen), dropped)
	}
}

func TestBroker_DroppedEventsLoggerHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var hookCount atomic.Int64
	b := broker.NewInMemoryBroker(broker.WithDroppedEventsLogger(func(record broker.DroppedEventRecord) {
		if record.Reason != broker.DropReasonSubscriberBufferFull {
			t.Errorf("hook reason = %q, want SUBSCRIBER_BUFFER_FULL", record.Reason)
		}
		hookCount.Add(1)
	}))

	const topic = "test.hook"
	events, err := b.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	go func() {
		for range events {
			time.Sleep(50 * time.Millisecond)
		}
	}()

	for i := 0; i < 128; i++ {
		if err := b.Publish(ctx, broker.Event{Topic: topic, Payload: i}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	hook := hookCount.Load()
	if hook == 0 {
		t.Fatal("expected DroppedEventsLogger hook to capture dropped events")
	}
	if hook != int64(len(b.DroppedRetentionRecords())) {
		t.Fatalf("hook count = %d, retention count = %d, want equal audit coverage",
			hook, len(b.DroppedRetentionRecords()))
	}
}

func TestFileBackedRetentionBuffer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drops.jsonl")

	buf, err := broker.NewFileBackedRetentionBuffer(path)
	if err != nil {
		t.Fatalf("NewFileBackedRetentionBuffer: %v", err)
	}
	defer buf.Close()

	droppedAt := time.Now()
	buf.Append(broker.DroppedEventRecord{
		Event: broker.Event{
			Topic:     "test.disk",
			Payload:   42,
			Timestamp: droppedAt.UnixMilli(),
		},
		Reason:            broker.DropReasonSubscriberBufferFull,
		DroppedAt:         droppedAt,
		DroppedAtUnixNano: droppedAt.UnixNano(),
	})

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected disk-backed retention file to contain dropped event record")
	}
}
