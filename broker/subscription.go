package broker

import "context"

// Subscription 是单个分区上的一个广播订阅者,维护独立游标。
//
// 状态与语义(V1a 起):
//   - cursor:下一条待读消息的 offset;
//   - committed:已确认水位,[0, committed) 的消息都被累积确认过;
//   - 不变量 0 ≤ committed ≤ cursor ≤ store.Len()。
//
// 并发模型:
//   - cursor / committed 逻辑上只被本订阅的单个读 goroutine 访问;
//   - 其读写与"读日志、判断关闭态"一起在 partition.mu 下完成:锁保护的是分区状态,
//     游标借此与日志进度保持一致,而非单独加锁;
//   - 唤醒 / 关闭信号分别经 ready、done 通道传递。
type Subscription struct {
	partition *Partition
	cursor    int64         // 下一条待读 offset,由读循环独占读写
	committed int64         // 已确认水位:[0, committed)
	ready     chan struct{} // cap-1:分区有新消息时的唤醒信号
	done      chan struct{} // 订阅被关闭时关闭,用于唤醒阻塞中的 Read
}

// Read 阻塞直到取到 [cursor, cursor+max) 中至少一条消息,返回这批消息并推进游标。
// max <= 0 时视为 1;max 超过日志剩余时按剩余夹紧(见 Store.Read),不会溢出。
// 同一 Subscription 同一时刻只能有一个 goroutine 调用 Read(单读者约定);
// 并发调用不会 data race,但会造成消息被重复返回、游标互相覆盖。
// 返回时机与错误:
//   - 有可读消息:返回消息切片(切片内容来自日志,不可修改),推进 s.cursor;
//   - 订阅或分区被关闭:返回 nil, ErrClosed;
//   - ctx 先被取消 / 超时:返回 nil, ctx.Err()。
//
// 读与游标推进都在 partition.mu 内完成,保证"先发布先可见"的次序:
// 一次发布要么被本批读到,要么在下次循环读到,绝不会漏读或乱序。
//
// 注意:若有可读消息,即使 ctx 已取消也优先返回数据;ctx 取消只在无数据阻塞等待时生效。
// ctx 取消返回 ctx.Err(),这只是"本次等待被打断",订阅仍然有效,可换 ctx 继续 Read;
// 而 ErrClosed 表示订阅或分区已终结,之后 Read 会持续返回 ErrClosed。
func (s *Subscription) Read(ctx context.Context, max int) ([]Message, error) {
	if max <= 0 {
		max = 1
	}

	p := s.partition
	for {
		p.mu.Lock()
		// 订阅(/分区)已关闭:立即返回,不再投递积压消息,保证 Close 能可靠截断消费。
		select {
		case <-s.done:
			p.mu.Unlock()
			return nil, ErrClosed
		default:
		}
		if p.closed {
			p.mu.Unlock()
			return nil, ErrClosed
		}
		msgs := p.store.Read(s.cursor, max)
		if len(msgs) > 0 {
			s.cursor = msgs[len(msgs)-1].Offset + 1
			p.mu.Unlock()
			return msgs, nil
		}
		p.mu.Unlock()

		// 无新数据:阻塞等待下一次唤醒、关闭或 ctx 取消。
		select {
		case <-s.ready:
		case <-s.done:
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Ack 累积确认到 upTo(含),返回 nil 表示成功;upTo < 0 返回 ErrInvalidAckOffset。
//
// 语义:
//   - 未来位点(upTo >= cursor)clamp 到 cursor,不能提交尚未投递的消息;
//   - 陈旧 ack(upTo+1 <= committed)为 no-op;
//   - 幂等:重复 ack 同一 upTo 等价于 no-op。
func (s *Subscription) Ack(upTo int64) error {
	if upTo < 0 {
		return ErrInvalidAckOffset
	}

	p := s.partition
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-s.done:
		return ErrClosed
	default:
	}
	if p.closed {
		return ErrClosed
	}

	newCommitted := s.cursor
	if upTo < s.cursor {
		newCommitted = upTo + 1
	}
	if newCommitted > s.committed {
		s.committed = newCommitted
	}
	return nil
}

// Close 注销订阅并唤醒任何阻塞的 Read(返回 ErrClosed)。
// 可安全重复调用:底层 unsubscribe 幂等;对已关闭分区产生的订阅同样成立。
func (s *Subscription) Close() error {
	s.partition.unsubscribe(s)
	return nil
}
