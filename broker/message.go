package broker

// Message 是分区日志中的一条消息,一经发布即为不可变。
// Payload 切片会在广播订阅者之间共享,发布后不得再修改其内容。
type Message struct {
	Offset  int64
	Payload []byte
}
