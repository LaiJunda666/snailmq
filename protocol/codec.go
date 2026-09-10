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

// getString 从 reader 读 [u32 长度][原始字节];长度不足或声明超长返回 ErrTruncated。
func getString(r *bytes.Reader) (string, error) {
	n, err := getUint32(r)
	if err != nil {
		return "", err
	}
	if int64(n) > int64(r.Len()) {
		return "", ErrTruncated
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", ErrTruncated
	}
	return string(buf), nil
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
func DecodePublish(p []byte) (topic string, payload []byte, err error) {
	r := bytes.NewReader(p)

	topic, err = getString(r)
	if err != nil {
		return "", nil, err
	}

	mlen, err := getUint32(r)
	if err != nil {
		return "", nil, err
	}
	if int64(mlen) > int64(r.Len()) {
		return "", nil, ErrTruncated
	}
	buf := make([]byte, mlen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", nil, ErrTruncated
	}
	return topic, buf, nil
}

// EncodeMessage 生成 OpMessage 的 payload:[int64 offset LE][u32 msgLen][msg]。
func EncodeMessage(offset int64, payload []byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, uint64(offset))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

// DecodeMessage 解析 OpMessage 的 payload,返回 offset 与消息体。
// 输入不足 12 字节(offset+长度)或消息体截断时返回 ErrTruncated。
func DecodeMessage(p []byte) (offset int64, payload []byte, err error) {
	r := bytes.NewReader(p)
	if r.Len() < 12 {
		return 0, nil, ErrTruncated
	}

	var off [8]byte
	var mlen [4]byte
	if _, err := io.ReadFull(r, off[:]); err != nil {
		return 0, nil, ErrTruncated
	}
	if _, err := io.ReadFull(r, mlen[:]); err != nil {
		return 0, nil, ErrTruncated
	}

	n := binary.LittleEndian.Uint32(mlen[:])
	if int64(n) > int64(r.Len()) {
		return 0, nil, ErrTruncated
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, ErrTruncated
	}
	return int64(binary.LittleEndian.Uint64(off[:])), buf, nil
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
