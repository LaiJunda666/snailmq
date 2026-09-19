// Package protocol 定义 SnailMQ 的二进制帧与各 opcode 的 payload 编解码。
//
// 纯编解码层:只吃 io.Reader / []byte,不依赖 broker 或网络,可独立测试。
// 帧格式:8 字节定长头(magic/version/opcode/payloadLen)+ payload。
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

// msgHeaderPool 复用 20 字节的 OpMessage 头(帧头 8 + offset 8 + 长度 4),
// 供 WriteMessageBatch 组装 writev 缓冲;头不逃逸出本次写,可安全复用。
var msgHeaderPool = sync.Pool{New: func() any { b := make([]byte, 20); return &b }}

const (
	Magic      uint16 = 0x4D51 // "MQ"
	Version    uint8  = 1
	HeaderLen         = 8
	MaxPayload        = 16 << 20 // 16 MiB 单帧 payload(帧体)上限
	// MaxMessage 是单条消息体的上限。推送帧 body = 8(offset)+4(长度)+msg,
	// 故取 MaxPayload-12,保证任何被接受发布的消息都能装进推送帧。
	MaxMessage = MaxPayload - 12
)

var (
	ErrInvalidMagic = errors.New("protocol: invalid magic")
	ErrBadVersion   = errors.New("protocol: unsupported version")
	ErrTruncated    = errors.New("protocol: truncated frame")
	ErrTooLarge     = errors.New("protocol: payload too large")
)

type Opcode uint8

const (
	OpCreateTopic  Opcode = 1
	OpPublish      Opcode = 2
	OpSubscribe    Opcode = 3
	OpMessage      Opcode = 4
	OpError        Opcode = 5
	OpPublishBatch Opcode = 6 // C→S:同一 topic 的多条消息,服务端回批量 ack
	OpPublishNoAck Opcode = 7 // C→S:fire-and-forget,服务端不回响应
)

// Code 是 OpError payload 中携带的结构化错误码,
// 让客户端可跨线用 errors.Is 判定错误类别,而不必匹配文本。
type Code uint16

const (
	CodeUnknown Code = iota
	CodeClosed
	CodeTopicExists
	CodeTopicNotFound
	CodeEmptyTopicName
	CodeTooLarge
	CodeOverloaded
)

func EncodeFrame(op Opcode, payload []byte) []byte {
	buf := make([]byte, HeaderLen+len(payload))
	binary.LittleEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = byte(op)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(payload)))
	copy(buf[8:], payload)
	return buf
}

func ReadHeader(r io.Reader) (Opcode, int, error) {
	var hdr [HeaderLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, 0, ErrTruncated
		}
		return 0, 0, err
	}

	if binary.LittleEndian.Uint16(hdr[0:2]) != Magic {
		return 0, 0, ErrInvalidMagic
	}
	if hdr[2] != Version {
		return 0, 0, ErrBadVersion
	}
	op := Opcode(hdr[3])
	// 先按 uint32 比较再转 int:32 位平台上 uint32 大值转 int 会变负数,
	// 直接转后再比较会绕过上限检查并导致下游 make 负长度 panic。
	payloadLen := binary.LittleEndian.Uint32(hdr[4:8])
	if payloadLen > MaxPayload {
		return 0, 0, ErrTooLarge
	}
	return op, int(payloadLen), nil
}

// ReadFrame 读取完整一帧:定长头 + payload,是 ReadHeader 之上的便捷封装。
// 头非法或 payload 截断时返回相应错误(后者为 ErrTruncated)。
func ReadFrame(r io.Reader) (Opcode, []byte, error) {
	op, n, err := ReadHeader(r)
	if err != nil {
		return 0, nil, err
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, ErrTruncated
	}
	return op, payload, nil
}

// WriteFrame 写入完整一帧。不负责 flush;调用方(如 bufio.Writer 使用者)按需 flush。
// payload 超过 MaxPayload 时本地拒收并返回 ErrTooLarge,避免写出非法帧。
func WriteFrame(w io.Writer, op Opcode, payload []byte) error {
	if len(payload) > MaxPayload {
		return ErrTooLarge
	}
	_, err := w.Write(EncodeFrame(op, payload))
	return err
}

// WriteFrameHeader 只写 8 字节定长头;payloadLen 超过 MaxPayload 返回 ErrTooLarge。
func WriteFrameHeader(w io.Writer, op Opcode, payloadLen int) error {
	if payloadLen > MaxPayload {
		return ErrTooLarge
	}
	var h [HeaderLen]byte
	binary.LittleEndian.PutUint16(h[0:2], Magic)
	h[2] = Version
	h[3] = byte(op)
	binary.LittleEndian.PutUint32(h[4:8], uint32(payloadLen))
	_, err := w.Write(h[:])
	return err
}

// WriteMessageBatch 一次写出多条 OpMessage:头(帧头+offset+长度)从池中复用,
// payload 直接引用,不拷贝;通过 net.Buffers 在 TCP 连接上走 writev(单次系统调用)。
// 返回的错误为第一条 ErrTooLarge(若有)或底层写错误。
func WriteMessageBatch(w io.Writer, offsets []int64, payloads [][]byte) error {
	if len(offsets) != len(payloads) {
		return errors.New("protocol: offsets/payloads length mismatch")
	}
	if len(offsets) == 0 {
		return nil
	}

	bufs := make(net.Buffers, 0, len(offsets)*2)
	hdrs := make([]*[]byte, 0, len(offsets))
	release := func() {
		for _, hp := range hdrs {
			msgHeaderPool.Put(hp)
		}
	}

	for i, off := range offsets {
		p := payloads[i]
		if len(p) > MaxMessage {
			release()
			return ErrTooLarge
		}
		hp := msgHeaderPool.Get().(*[]byte)
		h := *hp
		binary.LittleEndian.PutUint16(h[0:2], Magic)
		h[2] = Version
		h[3] = byte(OpMessage)
		binary.LittleEndian.PutUint32(h[4:8], uint32(12+len(p)))
		binary.LittleEndian.PutUint64(h[8:16], uint64(off))
		binary.LittleEndian.PutUint32(h[16:20], uint32(len(p)))
		bufs = append(bufs, h, p)
		hdrs = append(hdrs, hp)
	}

	_, err := bufs.WriteTo(w)
	release()
	return err
}

// WriteMessage 直接写出 OpMessage 帧(头 + offset + 长度 + payload),
// 避免 EncodeMessage/EncodeFrame 拼接整帧带来的额外分配与拷贝。
func WriteMessage(w io.Writer, offset int64, payload []byte) error {
	if len(payload) > MaxMessage {
		return ErrTooLarge
	}
	if err := WriteFrameHeader(w, OpMessage, 12+len(payload)); err != nil {
		return err
	}
	var meta [12]byte
	binary.LittleEndian.PutUint64(meta[0:8], uint64(offset))
	binary.LittleEndian.PutUint32(meta[8:12], uint32(len(payload)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// WritePublish 直接写出 OpPublish 帧(头 + topic 字段 + payload),避免中间拼接拷贝。
func WritePublish(w io.Writer, topic string, payload []byte) error {
	return writePublishOp(w, OpPublish, topic, payload)
}

// WritePublishNoAck 写出 OpPublishNoAck 帧(与 WritePublish 同格式,仅 opcode 不同);
// 服务端不会回响应,用于 fire-and-forget 发布。
func WritePublishNoAck(w io.Writer, topic string, payload []byte) error {
	return writePublishOp(w, OpPublishNoAck, topic, payload)
}

func writePublishOp(w io.Writer, op Opcode, topic string, payload []byte) error {
	bodyLen := 4 + len(topic) + 4 + len(payload)
	if bodyLen > MaxPayload {
		return ErrTooLarge
	}
	if err := WriteFrameHeader(w, op, bodyLen); err != nil {
		return err
	}
	var meta [4]byte
	binary.LittleEndian.PutUint32(meta[:], uint32(len(topic)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, topic); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(meta[:], uint32(len(payload)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// WritePublishBatch 直接写出 OpPublishBatch 帧:
// 头 + [u32 topicLen][topic] + [u32 count] + count * ([u32 msgLen][msg])。
// 全程分段直写,不拼接整帧,免去大批量下的额外拷贝。
func WritePublishBatch(w io.Writer, topic string, payloads [][]byte) error {
	bodyLen := 4 + len(topic) + 4
	for _, p := range payloads {
		if len(p) > MaxMessage {
			return ErrTooLarge
		}
		bodyLen += 4 + len(p)
	}
	if bodyLen > MaxPayload {
		return ErrTooLarge
	}
	if err := WriteFrameHeader(w, OpPublishBatch, bodyLen); err != nil {
		return err
	}
	var meta [4]byte
	binary.LittleEndian.PutUint32(meta[:], uint32(len(topic)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	if _, err := io.WriteString(w, topic); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(meta[:], uint32(len(payloads)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	for _, p := range payloads {
		binary.LittleEndian.PutUint32(meta[:], uint32(len(p)))
		if _, err := w.Write(meta[:]); err != nil {
			return err
		}
		if _, err := w.Write(p); err != nil {
			return err
		}
	}
	return nil
}
