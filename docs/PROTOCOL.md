# Protocol(v1)

## 帧格式(定长头 + payload)

```
byte 0-1   magic   uint16 LE = 0x4D51 ("MQ")
byte 2     version uint8  = 1
byte 3     opcode  uint8
byte 4-7   payloadLen uint32 LE
byte 8+    payload
```

## Opcode

| opcode | 方向 | payload |
|---|---|---|
| 1 OpCreateTopic | C→S | topic 名 |
| 2 OpPublish | C→S | topic(前缀 u32len)+ payload(前缀 u32len) |
| 3 OpSubscribe | C→S | topic 名 |
| 4 OpMessage | S→C | offset(int64 LE)+ payload(前缀 u32len) |
| 5 OpError | S→C | code(uint16 LE)+ 错误文本(前缀 u32len) |

- 成功响应:服务端回与请求相同 opcode,payload 为空;Publish 成功回 8 字节 offset
- 失败响应:OpError,payload 为结构化错误码 + 文本(见下)
- Subscribe 成功后,连接转为该订阅的推送流(逐条 OpMessage),直到连接关闭

## 错误码(OpError.code)

| code | 含义 |
|---|---|
| 0 | 未知 / 未分类 |
| 1 | 对象已关闭 |
| 2 | topic 已存在 |
| 3 | topic 不存在 |
| 4 | topic 名为空 |
| 5 | 帧/消息超限 |

## 消息长度上限

- 单帧 payload 上限 `MaxPayload = 16 MiB`
- 单条消息体上限 `MaxMessage = MaxPayload - 12`(推送帧 body = 8 字节 offset + 4 字节长度 + 消息体),
  保证任何被接受发布的消息都能装入推送帧

## 定长字符串字段

u32 长度前缀(LE)+ 原始字节。
