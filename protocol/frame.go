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
	MaxPayload        = 16 << 20 // 16 MiB 单帧上限
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
	payloadLen := int(binary.LittleEndian.Uint32(hdr[4:8]))
	if payloadLen > MaxPayload {
		return 0, 0, ErrTooLarge
	}
	return op, payloadLen, nil
}
