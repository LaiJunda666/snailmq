package protocol

import "testing"

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
