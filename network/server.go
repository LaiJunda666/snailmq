// Package network 提供 mq-lite 的标准库 TCP 服务端与客户端。
//
// 帧编解码复用 protocol 包,业务调用 broker 门面;依赖方向为 network → broker + protocol,
// broker 不反向依赖 network。服务端每连接一个 goroutine,订阅成功后该连接转为单向推送流。
package network

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

// Server 是 mq-lite 的 TCP 服务端:每条连接一个 goroutine,
// 订阅成功前走"请求-响应",订阅成功后该连接转为单向推送流(只发 OpMessage)。
type Server struct {
	broker *broker.Broker

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]context.CancelFunc // 活跃连接及其取消函数,用于 Close 时唤醒
	wg     sync.WaitGroup
}

// NewServer 用给定内核门面构造服务端。
func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
		conns:  make(map[net.Conn]context.CancelFunc),
	}
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
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			cancel()
			_ = conn.Close()
			return errors.New("network: server closed")
		}
		s.conns[conn] = cancel
		s.mu.Unlock()

		s.wg.Go(func() {
			s.handleConn(ctx, conn)
		})
	}
}

// Close 取消所有连接的上下文以唤醒阻塞中的推送流,并等待其退出。
// 幂等:重复调用返回 nil;不关闭 listener(listener 归调用方所有)。
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancels := make([]context.CancelFunc, 0, len(s.conns))
	for _, cancel := range s.conns {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	s.wg.Wait()
	return nil
}

// handleConn 循环处理一条连接的请求帧;一旦收到 OpSubscribe 并成功,
// 该连接转为推送流(stream)并在返回时关闭连接。
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	defer s.removeConn(conn)

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		op, plen, err := protocol.ReadHeader(br)
		if err != nil {
			return
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(br, payload); err != nil {
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
			if err := s.respond(bw, op, nil, nil); err != nil {
				return
			}
			// 连接语义变为单向推送流:只读内核、写帧,直到连接关闭 / 服务端 Close / 订阅关闭。
			s.stream(ctx, bw, sub)
			return
		default:
			// 客户端不应主动发 OpMessage / OpError,忽略。
			continue
		}
	}
}

// stream 从订阅读取消息并逐帧推送给客户端,直到 ctx 取消、订阅/分区关闭或写失败,
// 然后返回;调用方据此结束该连接。
func (s *Server) stream(ctx context.Context, bw *bufio.Writer, sub *broker.Subscription) {
	const batch = 64
	for {
		msgs, err := sub.Read(ctx, batch)
		if err != nil {
			return
		}
		for _, m := range msgs {
			frame := protocol.EncodeFrame(protocol.OpMessage, protocol.EncodeMessage(m.Offset, m.Payload))
			if _, err := bw.Write(frame); err != nil {
				return
			}
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
	if _, werr := bw.Write(protocol.EncodeFrame(op, payload)); werr != nil {
		return werr
	}
	return bw.Flush()
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
