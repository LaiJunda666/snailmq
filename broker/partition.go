package broker

import "sync"

// Partition 是单个分区:一段有序不可变的日志(经 Store 抽象)连同它的广播订阅者集合。
//
// 并发模型:分区内所有共享状态(store、subs、closed)都受 mu 保护;
// 需要跨 goroutine 传递的信号只走通道(订阅者的 ready/done),不另加锁。
// 因此 partition 上的并发访问全部串行化,不存在读者-写者竞态。
type Partition struct {
	mu     sync.Mutex
	store  Store                      // 分区日志后端,读写都必须在持有 mu 时进行
	subs   map[*Subscription]struct{} // 存活订阅者集合,用于注册与广播唤醒
	closed bool                       // 分区是否已关闭;关闭后拒绝 Publish/Subscribe
}

// newPartition 用给定的日志后端构造分区。store 由调用方(Broker)创建并保证非 nil。
func newPartition(store Store) *Partition {
	return &Partition{store: store, subs: make(map[*Subscription]struct{})}
}

// Publish 向分区日志追加一条消息,并尝试唤醒每个订阅者,返回该消息的 offset。
//
// 行为约定:
//   - 分区已关闭时返回 ErrClosed,消息不会写入日志。
//   - 追加成功后,在持有 mu 的同时向每个订阅者的 ready 发非阻塞信号(cap-1):
//     若订阅者尚未消费上一个信号,select 走 default 丢弃本次信号。
//     这是安全的有界丢弃——订阅者醒来后仍会以自身游标读取日志,
//     日志读取总会看到最新数据,因此不保证"每次发布恰好唤醒一次"。
func (p *Partition) Publish(payload []byte) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return 0, ErrClosed
	}

	off, err := p.store.Append(payload)
	if err != nil {
		return 0, err
	}

	for s := range p.subs {
		select {
		case s.ready <- struct{}{}:
		default:
		}
	}
	return off, nil
}

// Subscribe 注册一个新的广播订阅者,其游标从 offset 0 开始(可读全量历史)。
// 分区已关闭时返回 ErrClosed。返回的 Subscription 同一时刻只能被单个 goroutine
// 消费,调用方需自行保证(见 Subscription.Read)。
func (p *Partition) Subscribe() (*Subscription, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrClosed
	}

	s := &Subscription{
		partition: p,
		ready:     make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	p.subs[s] = struct{}{}
	return s, nil
}

// unsubscribe 注销一个订阅者:先从集合移除,再关闭其 done 通道以唤醒阻塞中的 Read。
// 幂等:若订阅者已不在集合(例如分区关闭时已被整体移除),则什么都不做。
func (p *Partition) unsubscribe(s *Subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.subs[s]; ok {
		delete(p.subs, s)
		close(s.done)
	}
}

// close 关闭分区:置 closed、唤醒并移除所有订阅者。
// 关闭后,订阅者的阻塞 Read 会收到 ErrClosed;后续的 Publish/Subscribe 也按关闭态拒绝。
// 关闭 done 的同时删除集合,可避免订阅者稍后 Close() 时对已关闭通道二次 close(panic)。
func (p *Partition) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	p.closed = true
	for s := range p.subs {
		close(s.done)
		delete(p.subs, s)
	}
}
