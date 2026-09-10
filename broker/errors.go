package broker

import "errors"

// ErrClosed 表示对象(分区 / 订阅 / broker)已关闭,相关操作被拒绝。
// 它由订阅读循环与各类门面方法统一返回;门面方法可能用 fmt.Errorf("%w")
// 附加"哪个操作 / 哪个 topic"的上下文,判断关闭态请始终用 errors.Is。
var ErrClosed = errors.New("broker: closed")

// ErrTopicExists 表示创建 topic 时同名 topic 已存在。
var ErrTopicExists = errors.New("broker: topic already exists")

// ErrTopicNotFound 表示 Publish/Subscribe 引用了不存在的 topic。
var ErrTopicNotFound = errors.New("broker: topic not found")
