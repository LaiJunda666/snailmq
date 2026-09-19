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
	// ErrOverloaded 表示服务端连接数已达上限,拒绝新连接。
	ErrOverloaded = errors.New("network: server overloaded")
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

// WithClientReadBuffer 设置连接的内核读缓冲字节数(0 表示用系统默认)。
func WithClientReadBuffer(n int) ClientOption {
	return func(c *Client) { c.readBuffer = n }
}

// WithClientWriteBuffer 设置连接的内核写缓冲字节数(0 表示用系统默认)。
func WithClientWriteBuffer(n int) ClientOption {
	return func(c *Client) { c.writeBuffer = n }
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
	readBuffer   int           // 内核读缓冲,0 用默认
	writeBuffer  int           // 内核写缓冲,0 用默认

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
		readBuffer:   defaultSocketBuffer,
		writeBuffer:  defaultSocketBuffer,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.tuneConn()
	return c, nil
}

// tuneConn 按配置调整连接的内核读写缓冲(仅 TCP 生效)。
func (c *Client) tuneConn() {
	if c.readBuffer <= 0 && c.writeBuffer <= 0 {
		return
	}
	tcp, ok := c.conn.(*net.TCPConn)
	if !ok {
		return
	}
	if c.readBuffer > 0 {
		_ = tcp.SetReadBuffer(c.readBuffer)
	}
	if c.writeBuffer > 0 {
		_ = tcp.SetWriteBuffer(c.writeBuffer)
	}
}

// CreateTopic 创建主题;失败(重名 / 空名 / 名称过长 / 服务端已关闭等)返回结构化错误。
func (c *Client) CreateTopic(name string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(name) > broker.MaxTopicNameLen {
		return fmt.Errorf("network: topic name too long (%d > %d)", len(name), broker.MaxTopicNameLen)
	}
	_, err := c.roundTrip(protocol.OpCreateTopic, []byte(name))
	return err
}

// Publish 向主题发布一条消息并返回其 offset。
// payload 超过 protocol.MaxMessage 或 topic 名过长时在本地拒绝。
func (c *Client) Publish(topic string, payload []byte) (int64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	if len(topic) > broker.MaxTopicNameLen {
		return 0, fmt.Errorf("network: topic name too long (%d > %d)", len(topic), broker.MaxTopicNameLen)
	}
	if len(payload) > protocol.MaxMessage {
		return 0, fmt.Errorf("network: payload %d exceeds MaxMessage %d: %w",
			len(payload), protocol.MaxMessage, protocol.ErrTooLarge)
	}
	body, err := c.roundTripWrite(protocol.OpPublish, func(w *bufio.Writer) error {
		return protocol.WritePublish(w, topic, payload)
	})
	if err != nil {
		return 0, err
	}
	off, err := protocol.UnmarshalOffset(body)
	if err != nil {
		return 0, fmt.Errorf("network: publish response: %w", err)
	}
	return off, nil
}

// PublishBatch 一次发布同一 topic 的多条消息,服务端以单帧 ack 返回首条 offset;
// 本批 offset 连续,即 [base, base+len(payloads))。比逐条 Publish 少 N-1 次往返。
func (c *Client) PublishBatch(topic string, payloads [][]byte) (base int64, err error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	if len(topic) > broker.MaxTopicNameLen {
		return 0, fmt.Errorf("network: topic name too long (%d > %d)", len(topic), broker.MaxTopicNameLen)
	}
	for i, p := range payloads {
		if len(p) > protocol.MaxMessage {
			return 0, fmt.Errorf("network: payload[%d] %d exceeds MaxMessage %d: %w",
				i, len(p), protocol.MaxMessage, protocol.ErrTooLarge)
		}
	}
	body, err := c.roundTripWrite(protocol.OpPublishBatch, func(w *bufio.Writer) error {
		return protocol.WritePublishBatch(w, topic, payloads)
	})
	if err != nil {
		return 0, err
	}
	base, count, err := protocol.DecodePublishAck(body)
	if err != nil {
		return 0, fmt.Errorf("network: publish-batch ack: %w", err)
	}
	if count != len(payloads) {
		return 0, fmt.Errorf("network: publish-batch ack count %d != %d", count, len(payloads))
	}
	return base, nil
}

// PublishAsync 以 fire-and-forget 方式发布一条消息:不等待 ack,失败也无法同步感知。
// 适用于可容忍丢失的吞吐场景;streaming 或已关闭时返回错误。
func (c *Client) PublishAsync(topic string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return ErrClosed
	}
	if c.streaming {
		return ErrStreaming
	}
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := protocol.WritePublishNoAck(c.bw, topic, payload); err != nil {
		return c.opErr(err)
	}
	return c.opErr(c.bw.Flush())
}

// Batcher 按 maxBatch / maxDelay 攒批后调用 PublishBatch,摊薄小消息的往返开销。
// Add 保存 payload 引用,调用方在批次 flush 前不得修改其内容。
type Batcher struct {
	c        *Client
	topic    string
	maxBatch int
	maxDelay time.Duration

	mu     sync.Mutex
	buf    [][]byte
	timer  *time.Timer
	err    error
	closed bool
}

// NewBatcher 创建攒批发布器;maxBatch<=0 视为 1,maxDelay<=0 表示不启用定时 flush。
func NewBatcher(c *Client, topic string, maxBatch int, maxDelay time.Duration) *Batcher {
	if maxBatch <= 0 {
		maxBatch = 1
	}
	return &Batcher{c: c, topic: topic, maxBatch: maxBatch, maxDelay: maxDelay}
}

// Add 追加一条消息;达到 maxBatch 时立即 flush 并返回其结果。
func (b *Batcher) Add(payload []byte) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
	b.buf = append(b.buf, payload)
	if len(b.buf) >= b.maxBatch {
		b.mu.Unlock()
		_, _, err := b.Flush()
		return err
	}
	if b.timer == nil && b.maxDelay > 0 {
		b.timer = time.AfterFunc(b.maxDelay, func() { _, _, _ = b.Flush() })
	}
	b.mu.Unlock()
	return nil
}

// Flush 立即发送当前缓冲批次,返回首条 offset 与条数;空缓冲返回 (0,0,上一次错误)。
func (b *Batcher) Flush() (int64, int, error) {
	b.mu.Lock()
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	if len(b.buf) == 0 {
		err := b.err
		b.mu.Unlock()
		return 0, 0, err
	}
	batch := b.buf
	b.buf = nil
	b.mu.Unlock()

	base, err := b.c.PublishBatch(b.topic, batch)
	b.mu.Lock()
	if err != nil {
		b.err = err
	}
	b.mu.Unlock()
	if err != nil {
		return 0, len(batch), err
	}
	return base, len(batch), nil
}

// Close 停止定时器并 flush 剩余消息;之后 Add 返回 ErrClosed。
func (b *Batcher) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	_, _, err := b.Flush()
	return err
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
	if _, err := c.roundTripWriteLocked(protocol.OpSubscribe, func(w *bufio.Writer) error {
		return protocol.WriteFrame(w, protocol.OpSubscribe, []byte(topic))
	}); err != nil {
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
	return c.roundTripWrite(op, func(w *bufio.Writer) error {
		return protocol.WriteFrame(w, op, payload)
	})
}

// roundTripWrite 与 roundTrip 相同,但由调用方决定如何写请求体(便于 Publish 直写、免拼接拷贝)。
func (c *Client) roundTripWrite(op protocol.Opcode, write func(*bufio.Writer) error) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, ErrClosed
	}
	if c.streaming {
		return nil, ErrStreaming
	}
	return c.roundTripWriteLocked(op, write)
}

// roundTripWriteLocked 是请求-响应的实现,调用方须持有 c.mu。
// 请求相位受读/写超时保护,避免服务端不响应时永久阻塞。
func (c *Client) roundTripWriteLocked(op protocol.Opcode, write func(*bufio.Writer) error) ([]byte, error) {
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := write(c.bw); err != nil {
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
	case protocol.CodeOverloaded:
		return fmt.Errorf("%s: %w", msg, ErrOverloaded)
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

// ReadBatch 读取一到多条推送消息:阻塞等待第一条,随后把已到达(缓冲内)的消息一并返回,
// 至多 max 条。用于减少逐条 Read 的调用开销;空闲时行为与 Read 相同。
func (s *Subscription) ReadBatch(max int) ([]broker.Message, error) {
	if max <= 0 {
		max = 1
	}
	first, err := s.Read()
	if err != nil {
		return nil, err
	}
	out := make([]broker.Message, 0, max)
	out = append(out, first)
	for len(out) < max && s.c.br.Buffered() > 0 {
		m, err := s.Read()
		if err != nil {
			break
		}
		out = append(out, m)
	}
	return out, nil
}
