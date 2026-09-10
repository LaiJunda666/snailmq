package protocol

import (
	"bytes"
	"errors"
	"testing"
)

// TestFrameRoundTrip 验证帧头编码后能被 ReadHeader 原样解析,且 payload 可读回。
func TestFrameRoundTrip(t *testing.T) {
	frame := EncodeFrame(OpPublish, []byte("hello"))
	r := bytes.NewReader(frame)

	op, plen, err := ReadHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpPublish || plen != 5 {
		t.Fatalf("op=%d plen=%d; want %d/5", op, plen, OpPublish)
	}

	payload := make([]byte, plen)
	if _, err := r.Read(payload); err != nil {
		t.Fatal(err)
	}
	if string(payload) != "hello" {
		t.Fatalf("payload=%q", payload)
	}
}

// TestReadHeaderBadMagic 验证 magic 不符时返回 ErrInvalidMagic。
func TestReadHeaderBadMagic(t *testing.T) {
	r := bytes.NewReader([]byte{0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00})
	if _, _, err := ReadHeader(r); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("err = %v; want ErrInvalidMagic", err)
	}
}

// TestReadHeaderBadVersion 验证 version 不符时返回 ErrBadVersion。
func TestReadHeaderBadVersion(t *testing.T) {
	r := bytes.NewReader([]byte{0x51, 0x4D, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00})
	if _, _, err := ReadHeader(r); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v; want ErrBadVersion", err)
	}
}

// TestPublishRoundTrip 验证 Publish payload(含中文 topic)编解码对称。
func TestPublishRoundTrip(t *testing.T) {
	body := EncodePublish("orders.中文", []byte("payload"))
	topic, payload, err := DecodePublish(body)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "orders.中文" || string(payload) != "payload" {
		t.Fatalf("topic=%q payload=%q", topic, payload)
	}
}

// TestMessageRoundTrip 验证 Message payload 编解码对称。
func TestMessageRoundTrip(t *testing.T) {
	body := EncodeMessage(42, []byte("m"))
	off, payload, err := DecodeMessage(body)
	if err != nil {
		t.Fatal(err)
	}
	if off != 42 || string(payload) != "m" {
		t.Fatalf("off=%d payload=%q", off, payload)
	}
}

// TestDecodePublishTruncated 验证截断输入返回错误而非 panic。
func TestDecodePublishTruncated(t *testing.T) {
	body := EncodePublish("topic", []byte("data"))
	if _, _, err := DecodePublish(body[:3]); err == nil {
		t.Fatal("want error on truncated input")
	}
}

// TestOffsetRoundTrip 验证 offset 的 8 字节编解码对称。
func TestOffsetRoundTrip(t *testing.T) {
	b := MarshalOffset(12345)
	off, err := UnmarshalOffset(b)
	if err != nil || off != 12345 {
		t.Fatalf("off=%d err=%v", off, err)
	}
}
