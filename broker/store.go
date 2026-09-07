package broker

import "sync"

// Store 是分区日志的抽象,隔离日志的存储与组织方式(内存起步,未来可换成 WAL 磁盘后端)。
// 方法语义见各注释;实现是否需要内部加锁由实现自行决定。
type Store interface {
	// Append 在日志尾部追加一条消息,返回其分配的递增 offset(从 0 开始)。
	Append(payload []byte) (int64, error)

	// Read 返回 [from, from+max) 区间内至多 max 条消息。
	// from < 0、from 越界或 max <= 0 时返回空切片;末尾不足 max 条时返回全部剩余。
	Read(from int64, max int) []Message

	// Len 返回当前已追加的消息总数,亦即下一条消息将获得的 offset。
	Len() int64
}

// memoryLog 是无界内存日志实现,内部用互斥锁保证并发安全。
// Read 返回的是独立 Message 值副本,调用方修改返回结果的 Offset 不会污染内部日志;
// 但 Payload 字节仍与发布者共享,须遵守不可变约定,不得写回。
type memoryLog struct {
	msgs []Message
	mu   sync.Mutex
}

// newMemoryLog 构造内存日志后端。零值即可用,无需预分配容量。
func newMemoryLog() Store {
	return &memoryLog{}
}

func (m *memoryLog) Append(payload []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	offset := int64(len(m.msgs))
	msg := Message{
		Offset:  offset,
		Payload: payload,
	}
	m.msgs = append(m.msgs, msg)

	return offset, nil
}

func (m *memoryLog) Read(from int64, max int) []Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	if from < 0 || from >= int64(len(m.msgs)) || max <= 0 {
		return nil
	}

	end := min(from+int64(max), int64(len(m.msgs)))

	result := make([]Message, end-from)
	copy(result, m.msgs[from:end])

	return result
}

func (m *memoryLog) Len() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return int64(len(m.msgs))
}
