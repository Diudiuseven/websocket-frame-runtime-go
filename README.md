# websocket-frame-runtime-go

一套覆盖完整链路的 WebSocket 连接运行时：帧编解码、消息分片重组、
心跳保活、permessage-deflate 压缩协商与关闭握手。
设计目标是在高并发与弱网条件下保持连接状态一致、行为可复现。

代码位于 `ws/` 包，仅依赖标准库。

## 快速开始

```go
// 在已完成 HTTP 升级握手的 net.Conn 上创建运行时。
cfg := ws.Config{
    MaxFramePayload:    1 << 20,
    MaxMessageSize:     16 << 20,
    HeartbeatInterval:  30 * time.Second,
    HeartbeatMaxMissed: 3,
    SendQueueSize:      64,
    SendBlock:          true,
    WriteTimeout:       10 * time.Second,
    CloseTimeout:       5 * time.Second,
    Compression:        ws.CompressConfig{Enabled: true, MinSize: 1024},
    IsClient:           true, // 客户端发送帧必须掩码
}
conn := ws.NewConn(nc, cfg, ext) // ext 为扩展协商结果
go conn.Run()

conn.Send(ws.OpText, []byte("hello"))
msg, err := conn.ReadMessage()
conn.Close(ws.CloseNormal, "bye")
ev := <-conn.Closed() // 恰好一个关闭事件
```

## 协议语义

### 帧编解码（frame.go）

- `FrameParser` 增量解析：任意截断、分次到达的字节流通过 `Feed` 喂入，
  数据不足时返回空结果等待后续字节，**不预分配负载、不越界读取**。
- 支持 7 位 / 16 位 / 64 位三种长度表示；非最简长度编码
  （如 16 位编码 <126 的值）按协议错误拒绝。
- 单帧负载超过 `MaxFramePayload` 拒绝（`ErrFrameTooLarge`）。
- 控制帧约束在解析与序列化两侧同时强制：不得分片（`ErrControlFragment`）、
  负载 ≤125 字节（`ErrControlTooLarge`）。
- 保留操作码拒绝（`ErrReservedOpCode`）。

### 消息分片与重组（message.go）

- 一条逻辑消息可跨多个数据帧；`Reassembler` 重组结果与未分片发送完全一致。
- 控制帧可插入分片序列中间，不影响重组状态。
- 孤立 continuation（`ErrUnexpectedContinuation`）、
  分片未完成时出现新数据帧（`ErrNewMessageInFragment`）按协议拒绝。
- 重组累计超过 `MaxMessageSize` 拒绝（`ErrMessageTooLarge`），
  内存占用以该上限为界。

### 压缩扩展（compress.go）

- 实现 permessage-deflate（RFC 7692），总是声明
  `client_no_context_takeover; server_no_context_takeover`，
  每条消息使用独立压缩上下文，行为可复现。
- 协商流程：客户端 `offerExtension` → 服务端 `acceptExtension` →
  客户端 `checkExtension`。本地未启用而对端响应扩展时返回
  `ErrExtensionNotNegotiated`。
- 协商结果（`extNegotiation`）在收发两个方向共用同一份，保证一致：
  发送侧置 RSV1 并压缩，接收侧校验 RSV1 并解压。
- 未协商却出现 RSV1 标记：连接以 `ReasonExtensionError` 终止。
- 解压输出受 `MaxMessageSize` 限制，防止解压炸弹造成无界内存分配。

### 心跳保活（conn.go）

- 按 `HeartbeatInterval` 周期发送 ping；每次收到 pong 将缺失计数清零。
- 连续缺失达到 `HeartbeatMaxMissed` 次（即约
  `HeartbeatInterval × HeartbeatMaxMissed` 无响应）后，
  以 `ReasonHeartbeatTimeout` + `CloseGoingAway` 终止连接。

### 关闭握手

- 双向推进：任一方收到关闭帧后回应一次关闭帧再终止；
  回应帧保证真正写出后才关闭底层连接，避免回应丢失。
- 幂等：`Close` 重复或并发调用只生效一次；`Closed()` 通道
  恰好收到一个 `CloseEvent` 后关闭。

## 关闭原因判定（CloseEvent.Reason）

| Reason | 含义 | 典型 Code |
|---|---|---|
| `ReasonLocal` | 本地调用 `Close` 发起的关闭完成（含对端不应答时超时收敛） | 本地传入的 code |
| `ReasonRemote` | 对端主动发起关闭握手 | 对端关闭帧中的 code |
| `ReasonHeartbeatTimeout` | 心跳连续缺失达到阈值 | `CloseGoingAway` |
| `ReasonProtocolError` | 帧格式/分片/控制帧约束违规 | `CloseProtocolError` |
| `ReasonExtensionError` | 未协商扩展却出现 RSV 标记 | `CloseProtocolError` |
| `ReasonMessageTooBig` | 帧或消息超过配置上限 | `CloseMessageTooBig` |
| `ReasonIOError` | 底层读写错误（弱网断链等） | `CloseAbnormal` |
| `ReasonWriteTimeout` | 发送侧超时（保留，当前归入 IOError） | `CloseAbnormal` |

## 并发与背压模型

- **读写分离**：读循环、写循环、心跳循环相互独立。
- **单写者**：所有帧经有界队列汇入唯一写循环，多生产者并发 `Send`
  不会出现帧内容交叉或半帧写出（由 `TestConnConcurrentSenders` 验证）。
- **背压**：`SendQueueSize` 限制在途帧数。`SendBlock=true` 时队列满阻塞
  （直到有空间或连接关闭）；`false` 时立即返回 `ErrSendQueueFull`。
- **收敛**：连接终止时阻塞中的发送者收到 `ErrConnClosed`，
  队列中未完成的发送被丢弃，内存有界。

## 资源占用上界

- 解析器缓冲 ≤ 单帧头部 + `MaxFramePayload`；负载只在完整到达后才分配。
- 重组缓冲 ≤ `MaxMessageSize`；解压输出 ≤ `MaxMessageSize`。
- 发送队列 ≤ `SendQueueSize` 个已序列化帧。
- 性能参考（`go test -bench .`，Xeon 8255C）：帧解析约 180 MB/s，
  序列化约 600 MB/s，解析与序列化均为 O(n) 单次拷贝。

## 测试

```sh
go test ./ws/ -race -count=1   # 全部测试（含竞态检测）
go test ./ws/ -bench .         # 基准
```

覆盖：三种长度表示与增量解析、非最简编码拒绝、控制帧约束、
分片重组等价性、乱序/插入控制帧、关闭握手双向与幂等、
心跳超时与健康保活、压缩协商与 RSV1 拒绝、
多生产者并发发送、背压队列满与阻塞收敛。
