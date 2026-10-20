package broker

import (
	"fmt"
	"sync"
	"testing"
)

// TestPublishBatchOffsetsContiguousUnderConcurrency 验证同 topic 多发布者并发时,
// 每个批次的 offset 仍连续占据 [base, base+len)。
func TestPublishBatchOffsetsContiguousUnderConcurrency(t *testing.T) {
	p := newPartition(newMemoryLog())
	const (
		publishers = 8
		perBatch   = 16
	)

	type batch struct {
		base     int64
		payloads []string
	}
	results := make(chan batch, publishers)
	var wg sync.WaitGroup
	for g := 0; g < publishers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			payloads := make([]string, perBatch)
			raw := make([][]byte, perBatch)
			for i := range payloads {
				payloads[i] = fmt.Sprintf("g%d-%d", g, i)
				raw[i] = []byte(payloads[i])
			}
			base, err := p.PublishBatch(raw)
			if err != nil {
				t.Errorf("publisher %d: %v", g, err)
				return
			}
			results <- batch{base: base, payloads: payloads}
		}(g)
	}
	wg.Wait()
	close(results)

	for b := range results {
		msgs := p.store.Read(b.base, len(b.payloads))
		if len(msgs) != len(b.payloads) {
			t.Fatalf("batch base=%d: got %d msgs; want %d", b.base, len(msgs), len(b.payloads))
		}
		for i, m := range msgs {
			if string(m.Payload) != b.payloads[i] {
				t.Fatalf("batch base=%d [%d] = %q; want %q (offsets not contiguous)",
					b.base, i, m.Payload, b.payloads[i])
			}
		}
	}
}
