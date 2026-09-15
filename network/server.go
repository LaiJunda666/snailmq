// Package network 提供 mq-lite 的标准库 TCP 服务端(客户端将在同一包内后续加入)。
//
// 帧编解码复用 protocol 包,业务调用 broker 门面;依赖方向为 network → broker + protocol,
// broker 不反向依赖 network。服务端每连接一个 goroutine,订阅成功后该连接转为单向推送流。
package network

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

const (
	// defaultReadTimeout 限制单次读帧(头 + payload)的等待时长,防空闲/慢速连接长期占用。
	defaultReadTimeout = 30 * time.Second
	// defaultWriteTimeout 限制单批推送的写入时长,写入超时则丢弃该慢订阅者连接。
	defaultWriteTimeout = 30 * time.Second
)

// Server 是 mq-lite 的 TCP 服务端:每条连接一个 goroutine,
// 订阅成功前走"请求-响应",订阅成功后该连接转为单向推送流(只发 OpMessage)。
type Server struct {
	broker *broker.Broker

	mu           sync.Mutex
	closed       bool
	conns        map[net.Conn]context.CancelFunc // 活跃连接及其取消函数,用于 Close 时唤醒与关闭
	wg           sync.WaitGroup
	readTimeout  time.Duration // 0 表示不设读超时
	writeTimeout time.Duration // 0 表示不设写超时
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

// NewServer 用给定内核门面构造服务端,并启用默认读/写超时。
func NewServer(b *broker.Broker, opts ...ServerOption) *Server {
	s := &Server{
		broker:       b,
		conns:        make(map[net.Conn]context.CancelFunc),
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
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
		s.conns[conn] = cancel
		s.wg.Go(func() {
			s.handleConn(ctx, conn)
		})
		s.mu.Unlock()
	}
}

// Close 取消并关闭所有连接,唤醒阻塞中的读/写与推送流,并等待其退出。
// 幂等:重复调用返回 nil;不关闭 listener(listener 归调用方所有)。
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
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
			return
		}

		switch op {
		case protocol.OpCreateTopic:
			if err := s.respond(bw, op, s.broker.CreateTopic(string(payload)), nil); err != nil {
				return
			}
		case protocol.OpPublish:
			topic, msg, derr := protocol.DecodePublish(payload)
			if derr != nil {
				if err := s.respond(bw, op, derr, nil); err != nil {
					return
				}
				continue
			}
			off, perr := s.broker.Publish(topic, msg)
			if err := s.respond(bw, op, perr, protocol.MarshalOffset(off)); err != nil {
				return
			}
		case protocol.OpSubscribe:
			sub, serr := s.broker.Subscribe(string(payload))
			if serr != nil {
				if err := s.respond(bw, op, serr, nil); err != nil {
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
			// 监视对端:客户端关闭连接(EOF)或发来异常数据时取消 subCtx,
			// 从而唤醒阻塞在 sub.Read 上的推送流并清理订阅。
			go func() {
				defer subCancel()
				var buf [256]byte
				for {
					if _, err := conn.Read(buf[:]); err != nil {
						return
					}
				}
			}()

			if err := s.respond(bw, op, nil, nil); err != nil {
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

// stream 从订阅读取消息并逐帧推送给客户端,直到 ctx 取消、订阅/分区关闭或写超时/写失败,
// 然后返回;调用方据此结束该连接。
func (s *Server) stream(ctx context.Context, conn net.Conn, bw *bufio.Writer, sub *broker.Subscription) {
	const batch = 64
	for {
		msgs, err := sub.Read(ctx, batch)
		if err != nil {
			return
		}
		for _, m := range msgs {
			frame := protocol.EncodeMessage(m.Offset, m.Payload)
			if err := protocol.WriteFrame(bw, protocol.OpMessage, frame); err != nil {
				return
			}
		}
		if !s.armWrite(conn) {
			return
		}
		if err := bw.Flush(); err != nil {
			return
		}
	}
}

// respond 写一个响应帧:成功时回与请求相同的 opcode 与 body;
// 失败时回 OpError,payload 为错误文本。返回写错误,调用方据此结束连接。
func (s *Server) respond(bw *bufio.Writer, op protocol.Opcode, err error, body []byte) error {
	var payload []byte
	if err != nil {
		op = protocol.OpError
		payload = []byte(err.Error())
	} else {
		payload = body
	}
	if werr := protocol.WriteFrame(bw, op, payload); werr != nil {
		return werr
	}
	return bw.Flush()
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

// connCount 返回当前活跃连接数(测试用于等待连接处理 goroutine 退出)。
func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
