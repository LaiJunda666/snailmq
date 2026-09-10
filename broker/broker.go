// Package broker 是 mq-lite 的内存版广播内核,不依赖网络层,可独立测试、可内嵌使用。
//
// 模型:每个 topic 一个分区(Partition),分区持有有序不可变日志(Store),
// 广播订阅者(Subscription)各持独立 offset 尾随读取。写入由分区持锁串行化,
// 新消息到达时经 cap-1 的 ready 通道唤醒阻塞中的订阅者;关闭经 done 通道传播。
//
// 并发与所有权:
//   - Broker / Partition 线程安全;Store 不自带并发安全,由分区持锁串行调用;
//   - 同一个 Subscription 同一时刻只能被单个 goroutine 调用 Read;
//   - Message.Payload 字节在订阅者之间共享,发布后不得修改。
//
// 日志后端抽象为 Store 接口,现仅提供内存实现 memoryLog;未来 WAL 磁盘实现
// 只要满足同一接口即可通过 WithStoreFactory 替换。
package broker

import (
	"errors"
	"fmt"
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

// CreateTopic 创建主题。同名返回 ErrTopicExists;空名返回 ErrTopicNameEmpty;
// broker 已关闭返回包装后的 ErrClosed(可用 errors.Is 判断)。
// 创建后才能对该主题 Publish / Subscribe。
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("broker: create topic %q: %w", name, ErrClosed)
	}

	if name == "" {
		return ErrTopicNameEmpty
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
// 主题不存在返回 ErrTopicNotFound;broker 已关闭返回包装后的 ErrClosed(可用 errors.Is 判断)。
func (b *Broker) Publish(topic string, payload []byte) (int64, error) {
	t, err := b.topic(topic)
	if err != nil {
		return 0, wrapClosed("publish to", topic, err)
	}

	return t.partition.Publish(payload)
}

// Subscribe 为主题注册一个新的广播订阅者(游标从 0 起,可读全量历史)。
// 返回的 Subscription 同一时刻只能被单个 goroutine 消费(见 Subscription.Read)。
// 主题不存在返回 ErrTopicNotFound;broker 已关闭返回包装后的 ErrClosed(可用 errors.Is 判断)。
func (b *Broker) Subscribe(topic string) (*Subscription, error) {
	t, err := b.topic(topic)
	if err != nil {
		return nil, wrapClosed("subscribe to", topic, err)
	}

	return t.partition.Subscribe()
}

// wrapClosed 仅在 err 为 ErrClosed 时补上操作与 topic 上下文,
// 保留 errors.Is 判定能力;其它错误原样返回。
func wrapClosed(op, topic string, err error) error {
	if errors.Is(err, ErrClosed) {
		return fmt.Errorf("broker: %s topic %q: %w", op, topic, err)
	}
	return err
}

// Close 关闭 broker:幂等,关闭所有主题的分区并唤醒其订阅者。
// 重复调用返回 nil;关闭后的其它操作(CreateTopic/Publish/Subscribe)返回包装后的 ErrClosed。
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	for _, t := range b.topics {
		t.partition.close()
	}

	b.topics = make(map[string]*Topic)

	return nil
}
