package broker

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestMemoryLogAppendAssignsSequentialOffsets(t *testing.T) {
	store := newMemoryLog()

	// 测试连续追加消息时偏移量是否正确
	testCases := []struct {
		name           string
		payload        []byte
		expectedOffset int64
	}{
		{
			name:           "first message",
			payload:        []byte("hello"),
			expectedOffset: 0,
		},
		{
			name:           "second message",
			payload:        []byte("world"),
			expectedOffset: 1,
		},
		{
			name:           "third message",
			payload:        []byte("test"),
			expectedOffset: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			offset, err := store.Append(tc.payload)
			if err != nil {
				t.Fatalf("Append() error = %v, want nil", err)
			}
			if offset != tc.expectedOffset {
				t.Errorf("Append() offset = %d, want %d", offset, tc.expectedOffset)
			}
		})
	}

	// 验证消息总数
	if got := store.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3", got)
	}
}

func TestMemoryLogRead(t *testing.T) {
	store := newMemoryLog()

	for i := range 5 {
		if _, err := store.Append(fmt.Appendf(nil, "message-%d", i)); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	tests := []struct {
		name string
		from int64
		max  int
		want []string
	}{
		{name: "read from beginning", from: 0, max: 2, want: []string{"message-0", "message-1"}},
		{name: "read from middle", from: 2, max: 2, want: []string{"message-2", "message-3"}},
		{name: "read with max exceeding available", from: 3, max: 10, want: []string{"message-3", "message-4"}},
		{name: "read from out of range", from: 10, max: 1, want: nil},
		{name: "read with negative from", from: -1, max: 1, want: nil},
		{name: "read with zero max", from: 0, max: 0, want: nil},
		{name: "read all messages", from: 0, max: 100, want: []string{"message-0", "message-1", "message-2", "message-3", "message-4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.Read(tt.from, tt.max)

			if len(got) != len(tt.want) {
				t.Errorf("Read() returned %d messages, want %d", len(got), len(tt.want))
				return
			}

			for i, msg := range got {
				if string(msg.Payload) != tt.want[i] {
					t.Errorf("Read() message[%d] = %s, want %s", i, msg.Payload, tt.want[i])
				}
				if msg.Offset != tt.from+int64(i) {
					t.Errorf("Read() message[%d] offset = %d, want %d", i, msg.Offset, tt.from+int64(i))
				}
			}
		})
	}
}

func TestMemoryLogLen(t *testing.T) {
	store := newMemoryLog()

	if got := store.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}

	for range 5 {
		if _, err := store.Append([]byte("test")); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if got := store.Len(); got != 5 {
		t.Errorf("Len() = %d, want 5", got)
	}
}

// TestMemoryLogConcurrency 让多个写者与读者同时操作同一日志,
// 在 -race 下验证内部锁能把重叠的 Append/Read 保护在竞争之外。
func TestMemoryLogConcurrency(t *testing.T) {
	store := newMemoryLog()

	const (
		writers   = 4
		perWriter = 250
		readers   = 4
		maxBatch  = 32
	)
	total := int64(writers * perWriter)

	stop := make(chan struct{})
	var readersWg sync.WaitGroup
	for range readers {
		readersWg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				msgs := store.Read(0, maxBatch)
				if len(msgs) > maxBatch {
					t.Errorf("Read() returned %d messages, want <= %d", len(msgs), maxBatch)
					return
				}
				for i, m := range msgs {
					if m.Offset < 0 || int64(m.Offset) >= total {
						t.Errorf("Read() offset %d out of range", m.Offset)
						return
					}
					if i > 0 && m.Offset != msgs[i-1].Offset+1 {
						t.Errorf("Read() offsets not contiguous: %d after %d", m.Offset, msgs[i-1].Offset)
						return
					}
				}
			}
		})
	}

	var writersWg sync.WaitGroup
	for range writers {
		writersWg.Go(func() {
			for j := 0; j < perWriter; j++ {
				if _, err := store.Append([]byte("message")); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		})
	}
	writersWg.Wait()
	close(stop)
	readersWg.Wait()

	if got := store.Len(); got != total {
		t.Errorf("Len() = %d, want %d", got, total)
	}
}

// TestMemoryLogReadReturnsFreshMessages 验证 Read 返回的是独立 Message 值:
// 调用方修改返回结果的 Offset 字段不会污染内部日志。
// Payload 字节按设计是共享的(不可变约定:发布后不得修改),故不在本文中写回。
func TestMemoryLogReadReturnsFreshMessages(t *testing.T) {
	store := newMemoryLog()

	payload := []byte("original message")
	if _, err := store.Append(payload); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	messages := store.Read(0, 1)
	if len(messages) != 1 {
		t.Fatalf("Read() returned %d messages, want 1", len(messages))
	}

	messages[0].Offset = 999

	messages2 := store.Read(0, 1)
	if len(messages2) != 1 {
		t.Fatalf("Read() returned %d messages, want 1", len(messages2))
	}

	if messages2[0].Offset != 0 {
		t.Errorf("Offset modified via returned message: got %d, want 0", messages2[0].Offset)
	}

	if !bytes.Equal(messages2[0].Payload, payload) {
		t.Errorf("Payload mismatch: got %s, want %s",
			string(messages2[0].Payload), string(payload))
	}
}
