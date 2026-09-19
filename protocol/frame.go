// Package protocol 定义 mq-lite 的二进制帧与各 opcode 的 payload 编解码。
//
// 纯编解码层:只吃 io.Reader / []byte,不依赖 broker 或网络,可独立测试。
// 帧格式:8 字节定长头(magic/version/opcode/payloadLen)+ payload。
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

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
	OpCreateTopic Opcode = 1
	OpPublish     Opcode = 2
	OpSubscribe   Opcode = 3
	OpMessage     Opcode = 4
	OpError       Opcode = 5
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
	bodyLen := 4 + len(topic) + 4 + len(payload)
	if bodyLen > MaxPayload {
		return ErrTooLarge
	}
	if err := WriteFrameHeader(w, OpPublish, bodyLen); err != nil {
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
