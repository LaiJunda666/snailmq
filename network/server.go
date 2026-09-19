// Package network 提供 SnailMQ 的标准库 TCP 服务端与客户端(客户端见 client.go)。
//
// 帧编解码复用 protocol 包,业务调用 broker 门面;依赖方向为 network → broker + protocol,
// broker 不反向依赖 network。服务端每连接一个 goroutine,订阅成功后该连接转为单向推送流。
package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/LaiJunda666/snailmq/broker"
	"github.com/LaiJunda666/snailmq/protocol"
)

const (
	// defaultReadTimeout 限制单次读帧(头 + payload)的等待时长,防空闲/慢速连接长期占用。
	defaultReadTimeout = 30 * time.Second
	// defaultWriteTimeout 限制单批推送的写入时长,写入超时则丢弃该慢订阅者连接。
	defaultWriteTimeout = 30 * time.Second
	// defaultSocketBuffer 是连接内核读写缓冲的默认值(可经选项覆盖;0 表示用系统默认)。
	defaultSocketBuffer = 256 << 10
)

// Server 是 SnailMQ 的 TCP 服务端:每条连接一个 goroutine,
// 订阅成功前走"请求-响应",订阅成功后该连接转为单向推送流(只发 OpMessage)。
type Server struct {
	broker *broker.Broker

	mu           sync.Mutex
	closed       bool
	conns        map[net.Conn]context.CancelFunc // 活跃连接及其取消函数,用于 Close 时唤醒与关闭
	wg           sync.WaitGroup
	readTimeout  time.Duration // 0 表示不设读超时
	writeTimeout time.Duration // 0 表示不设写超时
	maxConns     int           // 最大并发连接数,0 表示不限
	readBuffer   int           // 连接读缓冲字节数,0 表示用系统默认
	writeBuffer  int           // 连接写缓冲字节数,0 表示用系统默认

	closeOnce sync.Once
	closeDone chan struct{} // 关闭完成信号:所有 Close 调用者都等到同一点
}

// ServerOption 配置 Server,仅应在 NewServer 时传入。
type ServerOption func(*Server)

// WithReadTimeout 设置单次读帧的等待超时(0 表示不设)。
func WithReadTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.readTimeout = d }
}

// WithWriteTimeout 设置单批推送的写入超时(0 表示不设)。
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.writeTimeout = d }
}

// WithMaxConns 设置最大并发连接数(0 表示不限)。达到上限时新连接收到 OpError 后断开。
func WithMaxConns(n int) ServerOption {
	return func(s *Server) { s.maxConns = n }
}

// WithReadBuffer 设置每个连接的内核读缓冲字节数(0 表示用系统默认);
// 大消息高吞吐场景可放大以减少系统调用与窗口抖动。
func WithReadBuffer(n int) ServerOption {
	return func(s *Server) { s.readBuffer = n }
}

// WithWriteBuffer 设置每个连接的内核写缓冲字节数(0 表示用系统默认)。
func WithWriteBuffer(n int) ServerOption {
	return func(s *Server) { s.writeBuffer = n }
}

// NewServer 用给定内核门面构造服务端,并启用默认读/写超时。
func NewServer(b *broker.Broker, opts ...ServerOption) *Server {
	s := &Server{
		broker:       b,
		conns:        make(map[net.Conn]context.CancelFunc),
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		readBuffer:   defaultSocketBuffer,
		writeBuffer:  defaultSocketBuffer,
		closeDone:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Serve 阻塞地 accept 连接并为每条连接起一个 goroutine。
// Serve 不拥有 listener:ln 被外部 Close 时返回其错误。
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.tuneConn(conn)

		ctx, cancel := context.WithCancel(context.Background())
		// 登记与启动必须在同一临界区内完成:否则 Close 可能在登记后、wg.Go 前
		// 进入 Wait,导致 Wait 提前返回或违反 WaitGroup 的"Go 先于 Wait"约定。
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			cancel()
			_ = conn.Close()
			return errors.New("network: server closed")
		}
		if s.maxConns > 0 && len(s.conns) >= s.maxConns {
			s.mu.Unlock()
			cancel()
			// 满员:回一个结构化错误再断开,让对端有明确反馈(而非静默拒绝)。
			_ = protocol.WriteFrame(conn, protocol.OpError,
				protocol.EncodeError(protocol.CodeOverloaded, "network: too many connections"))
			_ = conn.Close()
			continue
		}
		s.conns[conn] = cancel
		s.wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("network: recovered panic in connection handler: %v", r)
				}
			}()
			s.handleConn(ctx, conn)
		})
		s.mu.Unlock()
	}
}

// Close 取消并关闭所有连接,唤醒阻塞中的读/写与推送流,并等待其退出。
// 幂等:重复调用返回 nil(若已有 Close 在进行,则等待其完成后再返回);
// 不关闭 listener(listener 归调用方所有)。
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		cancels := make([]context.CancelFunc, 0, len(s.conns))
		conns := make([]net.Conn, 0, len(s.conns))
		for conn, cancel := range s.conns {
			conns = append(conns, conn)
			cancels = append(cancels, cancel)
		}
		s.mu.Unlock()

		// 关连接可中断阻塞在 ReadHeader / Write 上的 goroutine;cancel 唤醒阻塞在 sub.Read 上的推送流。
		for i, conn := range conns {
			cancels[i]()
			_ = conn.Close()
		}
		s.wg.Wait()
		close(s.closeDone)
	})
	<-s.closeDone
	return nil
}

// handleConn 循环处理一条连接的请求帧;一旦收到 OpSubscribe 并成功,
// 该连接转为推送流(stream)并在返回时关闭连接与订阅。
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	defer s.removeConn(conn)

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		if !s.armRead(conn) {
			return
		}
		op, payload, err := protocol.ReadFrame(br)
		if err != nil {
			// 超大帧:回 OpError 让对端有明确反馈,再断开(剩余 payload 不读,直接关连接)。
			if errors.Is(err, protocol.ErrTooLarge) {
				_ = s.respond(conn, bw, protocol.OpError, protocol.ErrTooLarge, nil)
			}
			return
		}

		switch op {
		case protocol.OpCreateTopic:
			if err := s.respond(conn, bw, op, s.broker.CreateTopic(string(payload)), nil); err != nil {
				return
			}
		case protocol.OpPublish:
			topic, msg, derr := protocol.DecodePublish(payload)
			if derr != nil {
				if err := s.respond(conn, bw, op, derr, nil); err != nil {
					return
				}
				continue
			}
			if len(msg) > protocol.MaxMessage {
				// 超出可推送上限:直接拒绝,保证"发布成功 ⇒ 一定能推送",避免入库后推不出去。
				terr := fmt.Errorf("network: message exceeds MaxMessage (%d): %w", protocol.MaxMessage, protocol.ErrTooLarge)
				if err := s.respond(conn, bw, op, terr, nil); err != nil {
					return
				}
				continue
			}
			off, perr := s.broker.Publish(topic, msg)
			if err := s.respond(conn, bw, op, perr, protocol.MarshalOffset(off)); err != nil {
				return
			}
		case protocol.OpPublishBatch:
			topic, msgs, derr := protocol.DecodePublishBatch(payload)
			if derr != nil {
				if err := s.respond(conn, bw, op, derr, nil); err != nil {
					return
				}
				continue
			}
			base, perr := s.publishBatch(topic, msgs)
			if perr != nil {
				if err := s.respond(conn, bw, op, perr, nil); err != nil {
					return
				}
				continue
			}
			if err := s.respond(conn, bw, op, nil, protocol.EncodePublishAck(base, len(msgs))); err != nil {
				return
			}
		case protocol.OpPublishNoAck:
			topic, msg, derr := protocol.DecodePublish(payload)
			if derr == nil && len(msg) <= protocol.MaxMessage {
				_, _ = s.broker.Publish(topic, msg) // fire-and-forget:无响应,失败静默
			}
		case protocol.OpSubscribe:
			sub, serr := s.broker.Subscribe(string(payload))
			if serr != nil {
				if err := s.respond(conn, bw, op, serr, nil); err != nil {
					return
				}
				continue
			}
			// 推送相位不再需要读超时:清掉 deadline,交由"对端断开监视"决定生命周期。
			_ = conn.SetReadDeadline(time.Time{})

			subCtx, subCancel := context.WithCancel(ctx)
			defer subCancel()
			// 连接结束(客户断开/写失败/服务端 Close/分区关闭)时注销订阅,避免 Partition.subs 泄漏。
			defer func() { _ = sub.Close() }()
			// 监视对端:客户端关闭连接(EOF)、在推送相位发送数据(协议违规)或出错时
			// 取消 subCtx,从而唤醒阻塞在 sub.Read 上的推送流并清理订阅。
			monitorDone := make(chan struct{})
			go func() {
				defer close(monitorDone)
				defer subCancel()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("network: recovered panic in peer monitor: %v", r)
					}
				}()
				var buf [256]byte
				for {
					n, err := conn.Read(buf[:])
					if err != nil || n > 0 {
						return
					}
				}
			}()
			// 任何返回路径都关闭连接并 join monitor,确保其不活过本连接处理函数。
			defer func() {
				_ = conn.Close()
				<-monitorDone
			}()

			if err := s.respond(conn, bw, op, nil, nil); err != nil {
				return
			}
			// 连接语义变为单向推送流:只读内核、写帧,直到连接关闭 / 服务端 Close / 订阅关闭。
			s.stream(subCtx, conn, bw, sub)
			return
		default:
			// 客户端不应主动发 OpMessage / OpError,忽略。
			continue
		}
	}
}

// stream 从订阅读取消息并批量推送给客户端(writev),直到 ctx 取消、订阅/分区关闭或写失败。
func (s *Server) stream(ctx context.Context, conn net.Conn, bw *bufio.Writer, sub *broker.Subscription) {
	const batch = 64
	offsets := make([]int64, 0, batch)
	payloads := make([][]byte, 0, batch)
	for {
		msgs, err := sub.Read(ctx, batch)
		if err != nil {
			return
		}
		// 写 deadline 必须在整批写入之前设置:否则空闲后大批量写入会命中过期 deadline。
		if !s.armWrite(conn) {
			return
		}
		offsets = offsets[:0]
		payloads = payloads[:0]
		for _, m := range msgs {
			offsets = append(offsets, m.Offset)
			payloads = append(payloads, m.Payload)
		}
		if err := protocol.WriteMessageBatch(conn, offsets, payloads); err != nil {
			// 极端兜底:推送超限时先回 OpError 再断,避免订阅者只看到无解释的断连。
			if errors.Is(err, protocol.ErrTooLarge) {
				_ = s.respond(conn, bw, protocol.OpError, err, nil)
			}
			return
		}
	}
}

// publishBatch 顺序发布一批消息(同 topic),返回首条 offset;任一失败即返回该错误。
// 偏移在分区锁下连续分配,故整批 offset 为 [base, base+len)。
func (s *Server) publishBatch(topic string, msgs [][]byte) (int64, error) {
	var base int64
	for i, m := range msgs {
		if len(m) > protocol.MaxMessage {
			return 0, fmt.Errorf("network: message exceeds MaxMessage (%d): %w", protocol.MaxMessage, protocol.ErrTooLarge)
		}
		off, err := s.broker.Publish(topic, m)
		if err != nil {
			return 0, err
		}
		if i == 0 {
			base = off
		}
	}
	return base, nil
}

// respond 写一个响应帧:成功时回与请求相同的 opcode 与 body;
// 失败时回 OpError(结构化错误码 + 文本)。返回写错误,调用方据此结束连接。
// 写前设置写 deadline,避免不读响应的客户端把 goroutine 永久钉在写阻塞上。
func (s *Server) respond(conn net.Conn, bw *bufio.Writer, op protocol.Opcode, err error, body []byte) error {
	if !s.armWrite(conn) {
		return errors.New("network: set write deadline failed")
	}
	var payload []byte
	if err != nil {
		op = protocol.OpError
		payload = protocol.EncodeError(errorCode(err), err.Error())
	} else {
		payload = body
	}
	if werr := protocol.WriteFrame(bw, op, payload); werr != nil {
		return werr
	}
	return bw.Flush()
}

// errorCode 把 broker/protocol 的哨兵错误映射为协议错误码,供客户端结构化判定。
func errorCode(err error) protocol.Code {
	switch {
	case errors.Is(err, broker.ErrClosed):
		return protocol.CodeClosed
	case errors.Is(err, broker.ErrTopicExists):
		return protocol.CodeTopicExists
	case errors.Is(err, broker.ErrTopicNotFound):
		return protocol.CodeTopicNotFound
	case errors.Is(err, broker.ErrTopicNameEmpty):
		return protocol.CodeEmptyTopicName
	case errors.Is(err, protocol.ErrTooLarge):
		return protocol.CodeTooLarge
	default:
		return protocol.CodeUnknown
	}
}

// tuneConn 按配置调整连接的内核读写缓冲(仅 TCP 生效;0 表示不改)。
func (s *Server) tuneConn(conn net.Conn) {
	if s.readBuffer <= 0 && s.writeBuffer <= 0 {
		return
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if s.readBuffer > 0 {
		_ = tcp.SetReadBuffer(s.readBuffer)
	}
	if s.writeBuffer > 0 {
		_ = tcp.SetWriteBuffer(s.writeBuffer)
	}
}

// armRead 按配置设置读 deadline;返回 false 表示连接应结束(无超时配置时恒 true)。
func (s *Server) armRead(conn net.Conn) bool {
	if s.readTimeout <= 0 {
		return true
	}
	return conn.SetReadDeadline(time.Now().Add(s.readTimeout)) == nil
}

// armWrite 按配置设置写 deadline;返回 false 表示连接应结束。
func (s *Server) armWrite(conn net.Conn) bool {
	if s.writeTimeout <= 0 {
		return true
	}
	return conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)) == nil
}

// removeConn 注销连接并触发其取消(handleConn 退出时调用)。
func (s *Server) removeConn(conn net.Conn) {
	s.mu.Lock()
	if cancel, ok := s.conns[conn]; ok {
		cancel()
		delete(s.conns, conn)
	}
	s.mu.Unlock()
}
