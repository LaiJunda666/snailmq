package broker

import "errors"

// ErrClosed 表示对象(分区 / 订阅 / broker)已关闭。
//
// 它承担两种角色,调用方需按上下文区分:
//   - 拒绝类:Publish/Subscribe/CreateTopic 等写操作遇到已关闭对象,操作未发生,
//     通常意味着调用方生命周期管理有误;
//   - 终态类:Read 在阻塞中被关闭唤醒,属于订阅流的正常结束。
//
// 门面方法可能用 fmt.Errorf("%w") 附加"哪个操作 / 哪个 topic"的上下文;
// 判断关闭态请始终用 errors.Is(err, ErrClosed)。
var ErrClosed = errors.New("broker: closed")

// ErrTopicNameEmpty 表示 CreateTopic 传入了空主题名。
var ErrTopicNameEmpty = errors.New("broker: empty topic name")

// ErrTopicExists 表示创建 topic 时同名 topic 已存在。
var ErrTopicExists = errors.New("broker: topic already exists")

// ErrTopicNotFound 表示 Publish/Subscribe 引用了不存在的 topic。
var ErrTopicNotFound = errors.New("broker: topic not found")
