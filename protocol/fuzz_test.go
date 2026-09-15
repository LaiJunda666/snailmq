package protocol

import (
	"bytes"
	"testing"
)

// FuzzReadFrame 断言 ReadFrame 对任意输入都不 panic。
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0x51, 0x4D, 1, 1, 0, 0, 0, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ReadFrame(bytes.NewReader(data))
	})
}

// FuzzDecodePublish 断言 DecodePublish 对任意输入都不 panic。
func FuzzDecodePublish(f *testing.F) {
	f.Add([]byte("topicpayload"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodePublish(data)
	})
}

// FuzzDecodeMessage 断言 DecodeMessage 对任意输入都不 panic。
func FuzzDecodeMessage(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1, 'x'})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeMessage(data)
	})
}
