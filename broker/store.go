package broker

// Store 是分区日志的抽象,隔离日志的存储与组织方式(内存起步,未来可换成 WAL 磁盘后端)。
//
// 并发与调用约定:
//   - Store 不自带并发安全,也不保证方法间原子性;
//     所有方法必须由单一所有权方串行调用——本实现中即 Partition 在持 partition.mu 时调用。
//   - 日志按 offset 单调追加、只增不删;Append 返回的下一个 offset 即 Len()。
//   - 返回的 Message 必须视为只读:Payload 字节会在所有订阅者之间共享,
//     修改它会污染其他读者,调用方严禁写回。
type Store interface {
	// Append 在日志尾部追加一条消息,返回其分配的递增 offset(从 0 开始)。
	// payload 以引用方式保存,调用方发布后不得再修改其内容。
	Append(payload []byte) (int64, error)

	// Read 返回 [from, from+max) 区间内至多 max 条消息。
	// from < 0、from 越界或 max <= 0 时返回 nil;末尾不足 max 条时返回全部剩余;
	// max 超出日志剩余时按剩余夹紧,不会溢出。返回切片只读(见类型注释)。
	Read(from int64, max int) []Message

	// Len 返回当前已追加的消息总数,亦即下一条消息将获得的 offset。
	Len() int64
}

// memoryLog 是 Store 的内存实现:一段只增不删的 []Message。
//
// 不变量:offset 与数组下标一一对应(msgs[i].Offset == i),
// 因为日志无界、无清理、无空洞——订阅者可用"末条 offset+1"直接推进游标。
//
// 并发:不自带锁,依赖调用方(Partition)持锁串行访问。
// Read 返回独立分配的 Message 副本:修改返回值的 Offset 不污染内部,
// 但 Payload 字节与发布者共享,须遵守不可变约定,不得写回。
type memoryLog struct {
	msgs []Message
}

// newMemoryLog 构造内存日志后端。零值即可用,无需预分配容量。
func newMemoryLog() Store {
	return &memoryLog{}
}

// Append 追加一条消息并把当前长度当作其 offset;payload 以引用保存,不深拷贝。
func (m *memoryLog) Append(payload []byte) (int64, error) {
	offset := int64(len(m.msgs))
	msg := Message{
		Offset:  offset,
		Payload: payload,
	}
	m.msgs = append(m.msgs, msg)

	return offset, nil
}

// Read 语义同 Store.Read;具体地:
//   - from < 0、from 越界或 max <= 0 时返回 nil;
//   - 末尾不足 max 条时返回全部剩余,超大 max 按剩余夹紧(先算再截断,避免溢出);
//   - 命中时返回独立分配的切片(元素为 Message 副本),而非内部数组的窗口,
//     因此调用方对返回切片元素的写操作不会改到日志本体。
func (m *memoryLog) Read(from int64, max int) []Message {
	n := int64(len(m.msgs))
	if from < 0 || from >= n || max <= 0 {
		return nil
	}

	end := min(n, from+min(int64(max), n-from))
	result := make([]Message, end-from)
	copy(result, m.msgs[from:end])

	return result
}

// Len 返回当前已追加的消息总数,亦即下一条消息将获得的 offset。
func (m *memoryLog) Len() int64 {
	return int64(len(m.msgs))
}
