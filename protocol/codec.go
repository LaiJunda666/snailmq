package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
)

// putString 追加 [u32 长度][原始字节] 到 b。
func putString(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// getUint32 从 reader 读一个 u32;不足 4 字节返回 ErrTruncated。
func getUint32(r *bytes.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, ErrTruncated
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

// EncodePublish 生成 OpPublish 的 payload:[u32 topicLen][topic][u32 msgLen][msg]。
func EncodePublish(topic string, payload []byte) []byte {
	var b []byte
	b = putString(b, topic)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

// DecodePublish 解析 OpPublish 的 payload,返回 topic 与消息体。
// 输入截断或长度字段与实际不符时返回 ErrTruncated。
// 返回的 payload 是 p 的子切片(零拷贝):调用方不得修改,且 p 被复用时应视为失效。
func DecodePublish(p []byte) (topic string, payload []byte, err error) {
	if len(p) < 4 {
		return "", nil, ErrTruncated
	}
	tl := int64(binary.LittleEndian.Uint32(p[0:4]))
	pos := 4
	if tl > int64(len(p)-pos) {
		return "", nil, ErrTruncated
	}
	topic = string(p[pos : pos+int(tl)])
	pos += int(tl)

	if len(p)-pos < 4 {
		return "", nil, ErrTruncated
	}
	ml := int64(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	if ml > int64(len(p)-pos) {
		return "", nil, ErrTruncated
	}
	return topic, p[pos : pos+int(ml)], nil
}

// EncodeMessage 生成 OpMessage 的 payload:[int64 offset LE][u32 msgLen][msg]。
func EncodeMessage(offset int64, payload []byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(offset))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

// DecodeMessage 解析 OpMessage 的 payload,返回 offset 与消息体。
// 输入不足 12 字节(offset+长度)或消息体截断时返回 ErrTruncated。
// 返回的 payload 是 p 的子切片(零拷贝):调用方不得修改,且 p 被复用时应视为失效。
func DecodeMessage(p []byte) (offset int64, payload []byte, err error) {
	if len(p) < 12 {
		return 0, nil, ErrTruncated
	}
	offset = int64(binary.LittleEndian.Uint64(p[0:8]))
	ml := int64(binary.LittleEndian.Uint32(p[8:12]))
	if ml > int64(len(p)-12) {
		return 0, nil, ErrTruncated
	}
	return offset, p[12 : 12+int(ml)], nil
}

// MarshalOffset 将 offset 编码为 8 字节小端,用于 Publish 成功响应。
func MarshalOffset(o int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(o))
}

// UnmarshalOffset 解析 8 字节小端 offset;不足 8 字节返回 ErrTruncated。
func UnmarshalOffset(b []byte) (int64, error) {
	if len(b) < 8 {
		return 0, ErrTruncated
	}
	return int64(binary.LittleEndian.Uint64(b[:8])), nil
}

// EncodePublishAck 生成批量发布 ack 的 payload:[int64 base LE][u32 count LE]。
func EncodePublishAck(base int64, count int) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(base))
	return binary.LittleEndian.AppendUint32(b, uint32(count))
}

// DecodePublishAck 解析批量发布 ack;不足 12 字节返回 ErrTruncated。
func DecodePublishAck(b []byte) (base int64, count int, err error) {
	if len(b) < 12 {
		return 0, 0, ErrTruncated
	}
	return int64(binary.LittleEndian.Uint64(b[0:8])), int(binary.LittleEndian.Uint32(b[8:12])), nil
}

// EncodePublishBatch 生成 OpPublishBatch 的 payload:
// [u32 topicLen][topic][u32 count][u32 msgLen][msg]...。
func EncodePublishBatch(topic string, payloads [][]byte) []byte {
	b := putString(nil, topic)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payloads)))
	for _, p := range payloads {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(p)))
		b = append(b, p...)
	}
	return b
}

// DecodePublishBatch 解析 OpPublishBatch 的 payload。
// 返回的每个 payload 都是 p 的子切片(零拷贝);截断或计数与实际不符返回 ErrTruncated。
func DecodePublishBatch(p []byte) (topic string, payloads [][]byte, err error) {
	if len(p) < 4 {
		return "", nil, ErrTruncated
	}
	tl := int64(binary.LittleEndian.Uint32(p[0:4]))
	pos := 4
	if tl > int64(len(p)-pos) {
		return "", nil, ErrTruncated
	}
	topic = string(p[pos : pos+int(tl)])
	pos += int(tl)

	if len(p)-pos < 4 {
		return "", nil, ErrTruncated
	}
	count := int64(binary.LittleEndian.Uint32(p[pos : pos+4]))
	pos += 4
	if count < 0 || count > int64(len(p)-pos)/4 {
		return "", nil, ErrTruncated
	}

	payloads = make([][]byte, 0, count)
	for i := int64(0); i < count; i++ {
		if len(p)-pos < 4 {
			return "", nil, ErrTruncated
		}
		ml := int64(binary.LittleEndian.Uint32(p[pos : pos+4]))
		pos += 4
		if ml > int64(len(p)-pos) {
			return "", nil, ErrTruncated
		}
		payloads = append(payloads, p[pos:pos+int(ml)])
		pos += int(ml)
	}
	return topic, payloads, nil
}

// EncodeTopics 生成 OpListTopics 响应的 payload:[u32 count][ (u32 len)(name) ]*count。
func EncodeTopics(names []string) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(names)))
	for _, n := range names {
		b = putString(b, n)
	}
	return b
}

// DecodeTopics 解析 OpListTopics 响应;截断或计数与实际不符返回 ErrTruncated。
func DecodeTopics(p []byte) ([]string, error) {
	if len(p) < 4 {
		return nil, ErrTruncated
	}
	count := int64(binary.LittleEndian.Uint32(p[0:4]))
	pos := 4
	if count < 0 || count > int64(len(p)-pos)/4 {
		return nil, ErrTruncated
	}
	names := make([]string, 0, count)
	for i := int64(0); i < count; i++ {
		if len(p)-pos < 4 {
			return nil, ErrTruncated
		}
		n := int64(binary.LittleEndian.Uint32(p[pos : pos+4]))
		pos += 4
		if n > int64(len(p)-pos) {
			return nil, ErrTruncated
		}
		names = append(names, string(p[pos:pos+int(n)]))
		pos += int(n)
	}
	return names, nil
}

// EncodeError 生成 OpError 的 payload:[u16 code LE][u32 msgLen][msg]。
func EncodeError(code Code, msg string) []byte {
	b := binary.LittleEndian.AppendUint16(nil, uint16(code))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(msg)))
	return append(b, msg...)
}

// DecodeError 解析 OpError 的 payload,返回错误码与文本。
// 输入截断或长度字段与实际不符时返回 ErrTruncated。
func DecodeError(p []byte) (code Code, msg string, err error) {
	r := bytes.NewReader(p)
	var cb [2]byte
	if _, err := io.ReadFull(r, cb[:]); err != nil {
		return CodeUnknown, "", ErrTruncated
	}
	mlen, err := getUint32(r)
	if err != nil {
		return CodeUnknown, "", err
	}
	if int64(mlen) > int64(r.Len()) {
		return CodeUnknown, "", ErrTruncated
	}
	buf := make([]byte, mlen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return CodeUnknown, "", ErrTruncated
	}
	return Code(binary.LittleEndian.Uint16(cb[:])), string(buf), nil
}
