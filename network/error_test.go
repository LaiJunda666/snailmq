package network

import (
	"errors"
	"testing"

	"github.com/LaiJunda666/snailmq/protocol"
)

// TestRemoteErrorMapsInvalidSubscribe 验证客户端把 CodeInvalidSubscribe 映射回 protocol 哨兵。
func TestRemoteErrorMapsInvalidSubscribe(t *testing.T) {
	if !errors.Is(remoteError(protocol.CodeInvalidSubscribe, "x"), protocol.ErrInvalidSubscribe) {
		t.Fatal("CodeInvalidSubscribe should map to protocol.ErrInvalidSubscribe")
	}
}
