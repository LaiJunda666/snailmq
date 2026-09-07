package broker

import "errors"

// ErrClosed 表示对象(分区 / 订阅 / broker)已关闭,相关操作被拒绝。
// 它由订阅读循环与各类门面方法统一返回,调用方可用 errors.Is 判断关闭态。
var ErrClosed = errors.New("broker: closed")
