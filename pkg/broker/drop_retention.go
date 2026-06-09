package broker

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// DropReason identifies why an event was not delivered to a subscriber.
type DropReason string

const (
	// DropReasonSubscriberBufferFull is recorded when a subscriber channel is at capacity.
	DropReasonSubscriberBufferFull DropReason = "SUBSCRIBER_BUFFER_FULL"
)

// DroppedEventRecord is a structured audit entry for a shed broker event.
type DroppedEventRecord struct {
	Event             Event      `json:"event"`
	Reason            DropReason `json:"reason"`
	DroppedAt         time.Time  `json:"dropped_at"`
	DroppedAtUnixNano int64      `json:"dropped_at_unix_nano"`
}

// DroppedEventsLogger receives structured retention entries for dropped events.
type DroppedEventsLogger func(record DroppedEventRecord)

// DroppedEventRetentionBuffer stores dropped events for audit and optional disk persistence.
type DroppedEventRetentionBuffer struct {
	mu      sync.Mutex
	records []DroppedEventRecord
	file    *os.File
}

// NewDroppedEventRetentionBuffer creates an in-memory retention buffer.
func NewDroppedEventRetentionBuffer() *DroppedEventRetentionBuffer {
	return &DroppedEventRetentionBuffer{}
}

// NewFileBackedRetentionBuffer creates a retention buffer that appends JSON lines to path.
func NewFileBackedRetentionBuffer(path string) (*DroppedEventRetentionBuffer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &DroppedEventRetentionBuffer{file: f}, nil
}

// Append records a dropped event. Thread-safe.
func (buf *DroppedEventRetentionBuffer) Append(record DroppedEventRecord) {
	if buf == nil {
		return
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.records = append(buf.records, record)

	if buf.file != nil {
		line, err := json.Marshal(record)
		if err == nil {
			_, _ = buf.file.Write(append(line, '\n'))
		}
	}
}

// Len returns the number of retained drop records.
func (buf *DroppedEventRetentionBuffer) Len() int {
	if buf == nil {
		return 0
	}
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return len(buf.records)
}

// Records returns a copy of all retained drop records.
func (buf *DroppedEventRetentionBuffer) Records() []DroppedEventRecord {
	if buf == nil {
		return nil
	}
	buf.mu.Lock()
	defer buf.mu.Unlock()
	out := make([]DroppedEventRecord, len(buf.records))
	copy(out, buf.records)
	return out
}

// Close closes the optional disk backing file.
func (buf *DroppedEventRetentionBuffer) Close() error {
	if buf == nil || buf.file == nil {
		return nil
	}
	return buf.file.Close()
}
