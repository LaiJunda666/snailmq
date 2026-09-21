# Protocol(v1)

SnailMQ 的线协议:定长帧头 + 各 opcode 的 payload。所有整数为**小端(LE)**。

## 帧格式

```
byte 0-1   magic      uint16 LE = 0x4D51 ("MQ")
byte 2     version    uint8  = 1
byte 3     opcode     uint8
byte 4-7   payloadLen uint32 LE
byte 8+    payload
```

- 头部固定 8 字节;`payloadLen` 为 payload 字节数(不含头)。
- 单帧 `payloadLen` 上限 `MaxPayload = 16 MiB`,超限返回 `CodeTooLarge`。

## 消息长度上限

| 限制 | 值 | 说明 |
|---|---|---|
| 单帧 payload | 16 MiB | `MaxPayload` |
| 单条消息体 | `MaxMessage = MaxPayload - 12` | 推送帧 body = 8 offset + 4 len + msg,保证「发布成功 ⇒ 一定能推送」 |
| topic 名 | 255 字节 | `MaxTopicNameLen`(broker 层) |

## Opcode 与 payload

| opcode | 名称 | 方向 | payload 布局 |
|---|---|---|---|
| 1 | OpCreateTopic | C→S | `[u32 topicLen][topic]` |
| 2 | OpPublish | C→S | `[u32 topicLen][topic][u32 msgLen][msg]` |
| 3 | OpSubscribe | C→S | `[u32 topicLen][topic]` |
| 4 | OpMessage | S→C | `[int64 offset LE][u32 msgLen][msg]` |
| 5 | OpError | S→C | `[u16 code LE][u32 msgLen][msg]` |
| 6 | OpPublishBatch | C→S | `[u32 topicLen][topic][u32 count][ (u32 msgLen)(msg) ]*count` |
| 7 | OpPublishNoAck | C→S | `[u32 topicLen][topic][u32 msgLen][msg]`(无响应) |
| 8 | OpListTopics | C→S | 无;S→C 回 `[u32 count][ (u32 nameLen)(name) ]*count` |

所有 `u32 长度前缀` 采用「长度 + 原始字节」的定长字符串/块编码。

## 响应语义

- **成功**:服务端回与请求**相同 opcode**,payload:
  - `OpCreateTopic` / `OpSubscribe`:空
  - `OpPublish`:8 字节 offset(`int64 LE`)
  - `OpPublishBatch`:`[int64 base LE][u32 count]`,整批 offset 连续 `[base, base+count)`
- **失败**:回 `OpError`(`code + 文本`);客户端据 code 判定错误类别。
- **`OpPublishNoAck`**:服务端**不回任何响应**;解析/发布失败静默(fire-and-forget,可容忍丢失)。
- **`OpListTopics`**:成功响应为升序 topic 名列表;broker 已关闭回 `OpError`。

## 错误码(OpError.code)

| code | 含义 | 客户端映射 |
|---|---|---|
| 0 | 未知/未分类 | 文本错误 |
| 1 | 对象已关闭 | `broker.ErrClosed` |
| 2 | topic 已存在 | `broker.ErrTopicExists` |
| 3 | topic 不存在 | `broker.ErrTopicNotFound` |
| 4 | topic 名为空 | `broker.ErrTopicNameEmpty` |
| 5 | 帧/消息超限 | `protocol.ErrTooLarge` |
| 6 | 服务端过载(连接数超限) | `network.ErrOverloaded` |
| 7 | topic 名过长 | `broker.ErrTopicNameTooLong` |
| 8 | topic 名非法(含控制字符) | `broker.ErrInvalidTopicName` |
| 9 | topic 数量超限 | `broker.ErrTooManyTopics` |

## 连接状态机

```
请求相位(请求-响应)
   │  OpCreateTopic / OpPublish / OpPublishBatch / OpPublishNoAck / OpListTopics
   │  OpSubscribe ──► 回成功响应后
   ▼
推送相位(单向):服务端持续推送 OpMessage,直到:
   - 客户端关闭连接(EOF)/发送数据(协议违规)
   - 服务端 Close / 订阅关闭 / 分区关闭
   - 读/写超时(慢消费者被丢弃)
```

- 订阅成功后该连接**不能再发请求**(客户端实现会返回 `ErrStreaming`)。
- 未知 opcode:服务端当前忽略(不响应也不断开);官方客户端不会发送。

## 交互示例(发布 "hi" 到 "orders")

```
C→S  OpPublish payload = [00 00 00 06]"orders"[00 00 00 02]"hi"
S→C  OpPublish payload = [00 00 00 00 00 00 00 00]   (offset 0)
S→C  OpMessage payload = [offset=0][00 00 00 02]"hi"  (推送给订阅者)
```

## 版本与兼容

- 帧头 `version` 当前硬校验为 1;尚未引入握手/版本协商(V1 计划)。
- **新增 opcode / 错误码对旧客户端向后兼容**(旧客户端不会发送新请求);
  但**旧服务端不认识新 opcode**,按"未知 opcode 忽略、不响应"处理,新客户端调用旧服务端会阻塞到请求读超时
  (客户端请求相位默认 10s 超时可兜底)。
- 破坏性帧格式变更仍需 flag day;发布 `v1.0` 前冻结协议。
