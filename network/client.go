package network

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

// Client 是官方 Go 客户端(单连接)。
//
// 订阅前走"请求-响应";调用 Subscribe 成功后,该连接转为该订阅的推送流,
// 之后只能通过 Subscription.Read 读取推送,不能再发其他请求。
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	closeOnce sync.Once
	closeErr  error
}

// Dial 连接服务端并返回客户端。
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
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
func (c *Client) Subscribe(topic string) (*Subscription, error) {
	if _, err := c.roundTrip(protocol.OpSubscribe, []byte(topic)); err != nil {
		return nil, err
	}
	return &Subscription{c: c}, nil
}

// Close 关闭连接并唤醒阻塞中的订阅读。幂等:重复调用返回 nil。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// roundTrip 串行地发送请求帧并读取同序响应帧。
// 连接转为推送流(Subscribe 成功)后不得再调用。
func (c *Client) roundTrip(op protocol.Opcode, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
