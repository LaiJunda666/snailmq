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

// TestReadHeaderTooLarge 验证超长 payloadLen 被 ErrTooLarge 拒绝。
// 覆盖 32 位平台隐患:uint32 大值(0xFFFFFFFF)不得因转 int 变负而绕过检查。
func TestReadHeaderTooLarge(t *testing.T) {
	cases := map[string][]byte{
		"max+1":      {0x51, 0x4D, 1, 1, 0x01, 0x00, 0x00, 0x01}, // 16MiB+1
		"uint32 max": {0x51, 0x4D, 1, 1, 0xFF, 0xFF, 0xFF, 0xFF}, // 0xFFFFFFFF
	}
	for name, hdr := range cases {
		if _, _, err := ReadHeader(bytes.NewReader(hdr)); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("%s: err = %v; want ErrTooLarge", name, err)
		}
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

// TestErrorRoundTrip 验证 OpError payload 的错误码与文本编解码对称。
func TestErrorRoundTrip(t *testing.T) {
	body := EncodeError(CodeTopicNotFound, "broker: topic not found")
	code, msg, err := DecodeError(body)
	if err != nil {
		t.Fatal(err)
	}
	if code != CodeTopicNotFound || msg != "broker: topic not found" {
		t.Fatalf("code=%d msg=%q", code, msg)
	}
}

// TestDecodeErrorTruncated 验证截断的错误 payload 返回 ErrTruncated。
func TestDecodeErrorTruncated(t *testing.T) {
	body := EncodeError(CodeClosed, "x")
	if _, _, err := DecodeError(body[:3]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("err = %v; want ErrTruncated", err)
	}
}

// TestWriteMessageRoundTrip 验证 WriteMessage 直写出的帧可被读回解析。
func TestWriteMessageRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMessage(&buf, 42, []byte("m")); err != nil {
		t.Fatal(err)
	}
	op, body, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpMessage {
		t.Fatalf("op = %d; want OpMessage", op)
	}
	off, payload, err := DecodeMessage(body)
	if err != nil || off != 42 || string(payload) != "m" {
		t.Fatalf("off=%d payload=%q err=%v", off, payload, err)
	}
}

// TestWritePublishRoundTrip 验证 WritePublish 直写出的帧可被读回解析(含中文 topic)。
func TestWritePublishRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WritePublish(&buf, "orders.中文", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	op, body, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if op != OpPublish {
		t.Fatalf("op = %d; want OpPublish", op)
	}
	topic, payload, err := DecodePublish(body)
	if err != nil || topic != "orders.中文" || string(payload) != "payload" {
		t.Fatalf("topic=%q payload=%q err=%v", topic, payload, err)
	}
}

// TestDecodeZeroCopyAliasesInput 记录并验证零拷贝语义:返回的 payload 是输入的子切片。
func TestDecodeZeroCopyAliasesInput(t *testing.T) {
	body := EncodePublish("t", []byte("hello"))
	_, payload, err := DecodePublish(body)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'H' // 修改返回切片即修改输入(零拷贝);调用方须视其为只读。
	if body[len(body)-len(payload)] != 'H' {
		t.Fatal("payload is not a sub-slice of input; zero-copy expectation violated")
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
