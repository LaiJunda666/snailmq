package network

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LaiJunda666/mq-lite/broker"
	"github.com/LaiJunda666/mq-lite/protocol"
)

const (
	// defaultDialTimeout 是 Dial 的默认连接超时,避免对不可达地址无限等待。
	defaultDialTimeout = 5 * time.Second
	// defaultClientTimeout 是请求-响应相位的默认读/写超时(推送相位不设,空闲订阅合法)。
	defaultClientTimeout = 10 * time.Second
)

var (
	// ErrStreaming 表示客户端已订阅、连接进入推送流状态,不能再发起请求。
	ErrStreaming = errors.New("network: client is in streaming mode")
	// ErrClosed 表示客户端已关闭,后续操作被拒绝。
	ErrClosed = errors.New("network: client closed")
)

// ClientOption 配置 Client,仅应在 Dial/DialTimeout 时传入。
type ClientOption func(*Client)

// WithClientReadTimeout 设置请求-响应相位的读超时(0 表示不设)。
// 注意:进入推送流后的 Subscription.Read 不受此限制(空闲等待新消息是合法的)。
func WithClientReadTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.readTimeout = d }
}

// WithClientWriteTimeout 设置请求-响应相位的写超时(0 表示不设)。
func WithClientWriteTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.writeTimeout = d }
}

// Client 是官方 Go 客户端(单连接、单订阅)。
//
// 订阅前走"请求-响应";调用 Subscribe 成功后,该连接转为该订阅的推送流,
// 之后只能通过 Subscription.Read 读取推送,不能再发其他请求(会返回 ErrStreaming)。
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	streaming    bool          // 是否已进入推送流(受 mu 保护)
	readTimeout  time.Duration // 请求相位读超时,0 不设
	writeTimeout time.Duration // 请求相位写超时,0 不设

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Dial 用默认超时(5s)连接服务端并返回客户端。
func Dial(addr string, opts ...ClientOption) (*Client, error) {
	return DialTimeout(addr, defaultDialTimeout, opts...)
}

// DialTimeout 在指定超时内连接服务端;timeout <= 0 表示不设超时。
func DialTimeout(addr string, timeout time.Duration, opts ...ClientOption) (*Client, error) {
	if timeout < 0 {
		timeout = 0
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:         conn,
		br:           bufio.NewReader(conn),
		bw:           bufio.NewWriter(conn),
		readTimeout:  defaultClientTimeout,
		writeTimeout: defaultClientTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// CreateTopic 创建主题;失败(重名 / 空名 / 服务端已关闭等)返回结构化错误。
func (c *Client) CreateTopic(name string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	_, err := c.roundTrip(protocol.OpCreateTopic, []byte(name))
	return err
}

// Publish 向主题发布一条消息并返回其 offset。
// payload 超过 protocol.MaxMessage 时在本地拒绝并返回 protocol.ErrTooLarge。
func (c *Client) Publish(topic string, payload []byte) (int64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	if len(payload) > protocol.MaxMessage {
		return 0, fmt.Errorf("network: payload %d exceeds MaxMessage %d: %w",
			len(payload), protocol.MaxMessage, protocol.ErrTooLarge)
	}
	body, err := c.roundTrip(protocol.OpPublish, protocol.EncodePublish(topic, payload))
	if err != nil {
		return 0, err
	}
	off, err := protocol.UnmarshalOffset(body)
	if err != nil {
		return 0, fmt.Errorf("network: publish response: %w", err)
	}
	return off, nil
}

// Subscribe 订阅主题;成功后本连接的语义变为单向推送流。
// 关闭后返回 ErrClosed;重复订阅或在推送流状态下发起请求会返回 ErrStreaming。
func (c *Client) Subscribe(topic string) (*Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.streaming {
		return nil, ErrStreaming
	}
	if _, err := c.roundTripLocked(protocol.OpSubscribe, []byte(topic)); err != nil {
		return nil, err
	}
	c.streaming = true
	// 推送相位不设读超时:清掉请求相位遗留的 deadline,空闲等新消息是合法的。
	_ = c.conn.SetReadDeadline(time.Time{})
	return &Subscription{c: c}, nil
}

// Close 关闭连接并唤醒阻塞中的订阅读。幂等:重复调用返回 nil。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// roundTrip 串行地发送请求帧并读取同序响应帧;关闭或已进入推送流时拒绝。
func (c *Client) roundTrip(op protocol.Opcode, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.streaming {
		return nil, ErrStreaming
	}
	return c.roundTripLocked(op, payload)
}

// roundTripLocked 是 roundTrip 的实现,调用方须持有 c.mu。
// 请求相位受读/写超时保护,避免服务端不响应时永久阻塞。
func (c *Client) roundTripLocked(op protocol.Opcode, payload []byte) ([]byte, error) {
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := protocol.WriteFrame(c.bw, op, payload); err != nil {
		return nil, c.opErr(err)
	}
	if err := c.bw.Flush(); err != nil {
		return nil, c.opErr(err)
	}

	if c.readTimeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	respOp, body, err := protocol.ReadFrame(c.br)
	if err != nil {
		return nil, c.opErr(err)
	}
	switch respOp {
	case protocol.OpError:
		code, msg, derr := protocol.DecodeError(body)
		if derr != nil {
			return nil, fmt.Errorf("network: malformed error response: %w", derr)
		}
		return nil, remoteError(code, msg)
	case op:
		return body, nil
	default:
		return nil, fmt.Errorf("network: unexpected opcode %d", respOp)
	}
}

// opErr 在连接因 Close 而失败时归一为 ErrClosed,其余原样返回。
func (c *Client) opErr(err error) error {
	if c.closed.Load() {
		return ErrClosed
	}
	return err
}

// remoteError 把协议错误码映射回本地哨兵错误,保留 errors.Is 判定;未知码退化为文本错误。
func remoteError(code protocol.Code, msg string) error {
	switch code {
	case protocol.CodeClosed:
		return fmt.Errorf("%s: %w", msg, broker.ErrClosed)
	case protocol.CodeTopicExists:
		return fmt.Errorf("%s: %w", msg, broker.ErrTopicExists)
	case protocol.CodeTopicNotFound:
		return fmt.Errorf("%s: %w", msg, broker.ErrTopicNotFound)
	case protocol.CodeEmptyTopicName:
		return fmt.Errorf("%s: %w", msg, broker.ErrTopicNameEmpty)
	case protocol.CodeTooLarge:
		return fmt.Errorf("%s: %w", msg, protocol.ErrTooLarge)
	default:
		return errors.New(msg)
	}
}

// Subscription 消费单个订阅的推送流(逐条 OpMessage)。
type Subscription struct {
	c *Client
}

// Read 阻塞读取下一条推送消息;客户端关闭时返回 ErrClosed。
// 推送相位不设读超时:空闲等待新消息是合法行为,由 Close 打断。
func (s *Subscription) Read() (broker.Message, error) {
	c := s.c
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return broker.Message{}, ErrClosed
	}
	op, body, err := protocol.ReadFrame(c.br)
	if err != nil {
		return broker.Message{}, c.opErr(err)
	}
	switch op {
	case protocol.OpMessage:
		off, payload, derr := protocol.DecodeMessage(body)
		if derr != nil {
			return broker.Message{}, fmt.Errorf("network: pushed message: %w", derr)
		}
		return broker.Message{Offset: off, Payload: payload}, nil
	case protocol.OpError:
		code, msg, derr := protocol.DecodeError(body)
		if derr != nil {
			return broker.Message{}, fmt.Errorf("network: malformed error response: %w", derr)
		}
		return broker.Message{}, remoteError(code, msg)
	default:
		return broker.Message{}, fmt.Errorf("network: unexpected opcode %d", op)
	}
}
