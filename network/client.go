package network

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

// defaultDialTimeout 是 Dial 的默认连接超时,避免对不可达地址无限等待。
const defaultDialTimeout = 5 * time.Second

// ErrStreaming 表示客户端已订阅、连接进入推送流状态,不能再发起请求。
var ErrStreaming = errors.New("network: client is in streaming mode")

// Client 是官方 Go 客户端(单连接)。
//
// 订阅前走"请求-响应";调用 Subscribe 成功后,该连接转为该订阅的推送流,
// 之后只能通过 Subscription.Read 读取推送,不能再发其他请求(会返回 ErrStreaming)。
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	streaming bool // 是否已进入推送流(受 mu 保护)

	closeOnce sync.Once
	closeErr  error
}

// Dial 用默认超时(5s)连接服务端并返回客户端。
func Dial(addr string) (*Client, error) {
	return DialTimeout(addr, defaultDialTimeout)
}

// DialTimeout 在指定超时内连接服务端;timeout <= 0 表示不设超时。
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		br:   bufio.NewReader(conn),
		bw:   bufio.NewWriter(conn),
	}, nil
}

// CreateTopic 创建主题;失败(重名 / 空名 / 服务端已关闭等)返回服务端错误。
func (c *Client) CreateTopic(name string) error {
	_, err := c.roundTrip(protocol.OpCreateTopic, []byte(name))
	return err
}

// Publish 向主题发布一条消息并返回其 offset。
func (c *Client) Publish(topic string, payload []byte) (int64, error) {
	body, err := c.roundTrip(protocol.OpPublish, protocol.EncodePublish(topic, payload))
	if err != nil {
		return 0, err
	}
	return protocol.UnmarshalOffset(body)
}

// Subscribe 订阅主题;成功后本连接的语义变为单向推送流。
// 重复订阅或在推送流状态下发起请求会返回 ErrStreaming。
func (c *Client) Subscribe(topic string) (*Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.streaming {
		return nil, ErrStreaming
	}
	if _, err := c.roundTripLocked(protocol.OpSubscribe, []byte(topic)); err != nil {
		return nil, err
	}
	c.streaming = true
	return &Subscription{c: c}, nil
}

// Close 关闭连接并唤醒阻塞中的订阅读。幂等:重复调用返回 nil。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// roundTrip 串行地发送请求帧并读取同序响应帧;已进入推送流后拒绝请求。
func (c *Client) roundTrip(op protocol.Opcode, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.streaming {
		return nil, ErrStreaming
	}
	return c.roundTripLocked(op, payload)
}

// roundTripLocked 是 roundTrip 的实现,调用方须持有 c.mu。
func (c *Client) roundTripLocked(op protocol.Opcode, payload []byte) ([]byte, error) {
	if err := protocol.WriteFrame(c.bw, op, payload); err != nil {
		return nil, err
	}
	if err := c.bw.Flush(); err != nil {
		return nil, err
	}

	respOp, body, err := protocol.ReadFrame(c.br)
	if err != nil {
		return nil, err
	}
	switch respOp {
	case protocol.OpError:
		return nil, errors.New(string(body))
	case op:
		return body, nil
	default:
		return nil, fmt.Errorf("network: unexpected opcode %d", respOp)
	}
}

// Subscription 消费单个订阅的推送流(逐条 OpMessage)。
type Subscription struct {
	c *Client
}

// Read 阻塞读取下一条推送消息;连接关闭或收到服务端错误时返回 error。
func (s *Subscription) Read() (broker.Message, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	op, body, err := protocol.ReadFrame(s.c.br)
	if err != nil {
		return broker.Message{}, err
	}
	switch op {
	case protocol.OpMessage:
		off, payload, err := protocol.DecodeMessage(body)
		if err != nil {
			return broker.Message{}, err
		}
		return broker.Message{Offset: off, Payload: payload}, nil
	case protocol.OpError:
		return broker.Message{}, errors.New(string(body))
	default:
		return broker.Message{}, fmt.Errorf("network: unexpected opcode %d", op)
	}
}
