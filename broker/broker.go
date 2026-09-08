package broker

import (
	"errors"
	"sync"
)

// Option 用于配置 Broker,仅应在 New 时传入。
type Option func(*Broker)

// WithStoreFactory 注入日志后端工厂(默认内存实现)。
// 传入 nil 等价于不注入,仍使用默认 newMemoryLog。
// 供未来 WAL 磁盘后端替换与测试注入。
func WithStoreFactory(f func() Store) Option {
	return func(b *Broker) { b.newStore = f }
}

// Broker 是内核门面,组织多个 topic(每 topic 一个分区),线程安全,不依赖网络。
//
// 用法:New 创建后先 CreateTopic,再对该 topic Publish / Subscribe;
// Close 幂等,关闭后所有方法(除重复 Close)返回 ErrClosed。
type Broker struct {
	mu       sync.RWMutex
	closed   bool
	topics   map[string]*Topic
	newStore func() Store
}

// Topic 是一个已创建的消息主题,内部承载其唯一的 Partition。
type Topic struct {
	name      string
	partition *Partition
}

// New 创建 Broker。opts 可注入日志后端等配置。
func New(opts ...Option) *Broker {
	b := &Broker{
		topics:   make(map[string]*Topic),
		newStore: newMemoryLog,
	}
	for _, o := range opts {
		o(b)
	}
	if b.newStore == nil {
		b.newStore = newMemoryLog
	}
	return b
}

// CreateTopic 创建主题。同名返回 ErrTopicExists;空名返回错误;
// broker 已关闭返回 ErrClosed。创建后才能对该主题 Publish / Subscribe。
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}

	if name == "" {
		return errors.New("broker: empty topic name")
	}

	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}

	b.topics[name] = &Topic{
		name:      name,
		partition: newPartition(b.newStore()),
	}
	return nil
}

func (b *Broker) topic(name string) (*Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrClosed
	}

	t, ok := b.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

// Publish 向主题追加一条消息并返回其 offset。
// payload 以引用方式入队,广播订阅者共享其字节;
// 调用方在 Publish 返回后不得修改或复用该切片内容。
// 主题不存在返回 ErrTopicNotFound;broker 已关闭返回 ErrClosed。
func (b *Broker) Publish(topic string, payload []byte) (int64, error) {
	t, err := b.topic(topic)
	if err != nil {
		return 0, err
	}

	return t.partition.Publish(payload)
}

// Subscribe 为主题注册一个新的广播订阅者(游标从 0 起,可读全量历史)。
// 返回的 Subscription 同一时刻只能被单个 goroutine 消费(见 Subscription.Read)。
// 主题不存在返回 ErrTopicNotFound;broker 已关闭返回 ErrClosed。
func (b *Broker) Subscribe(topic string) (*Subscription, error) {
	t, err := b.topic(topic)
	if err != nil {
		return nil, err
	}

	return t.partition.Subscribe()
}

// Close 关闭 broker:幂等,关闭所有主题的分区并唤醒其订阅者。
// 首次调用返回 nil;再次调用及关闭后的任何操作返回 ErrClosed。
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}
	b.closed = true

	for _, t := range b.topics {
		t.partition.close()
	}

	b.topics = make(map[string]*Topic)

	return nil
}
