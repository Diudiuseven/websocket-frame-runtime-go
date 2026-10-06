# websocket-frame-runtime-go

一套覆盖完整链路的 WebSocket 连接运行时（RFC 6455 + RFC 7692），位于 `ws` 包：

- 帧编解码：增量解析 / 序列化，三种长度表示，掩码处理
- 消息分片与重组，控制帧约束校验
- 双向、幂等的关闭握手，关闭原因可区分、可观测
- 心跳保活（ping/pong），连续缺失按策略终止
- 可选 permessage-deflate 压缩扩展协商
- 读写分离、多生产者并发发送、有界发送队列背压

## 快速开始

```go
srv := ws.NewConn(serverConn, ws.Config{IsServer: true})
cli := ws.NewConn(clientConn, ws.Config{
    Heartbeat: &ws.HeartbeatConfig{Interval: 30 * time.Second},
    Compress:  &ws.CompressConfig{Enabled: true},
    CompressionActive: true, // 由握手层协商后填入
})

_ = cli.Send(ws.OpText, []byte("hello"))          // 多 goroutine 可并发调用
_ = cli.SendFragmented(ws.OpBinary, big, 64<<10)   // 分片发送，原子不交叉
msg, err := cli.ReadMessage()                      // 阻塞读取重组后的完整消息
_ = cli.Close()                                    // 幂等关闭握手
```

## 协议语义

### 帧解析（frame.go）

`FrameParser` 从任意截断、分次到达的字节流增量解析：未完整的帧保留在内部
缓冲区等待后续字节，不误判、不越界。缓冲区上界为「一个不完整帧」，即
`MaxFramePayload + 14` 字节头。解析时强制校验：

- RSV2/RSV3 必须为零（无对应扩展）；RSV1 仅在协商了压缩时合法（连接层判定）
- 操作码必须是已定义值，否则 `ErrReservedOpcode`
- 长度必须使用最短表示（7 位 → 16 位 → 64 位），64 位长度最高位必须为零，
  否则 `ErrInvalidLength`
- 控制帧不得分片（`ErrControlFragmented`），负载 ≤ 125 字节
  （`ErrControlPayloadSize`）
- 单帧负载超过 `MaxFramePayload` 拒绝（`ErrFrameTooLarge`）

`EncodeFrame` 总是写出最短长度表示；`Masked` 为真且掩码键为零时自动生成
随机掩码。

### 消息重组（message.go）

`Reassembler` 把分片序列重组为逻辑消息：

- 分片必须由数据帧（text/binary）开始、零个或多个 continuation 接续、
  Fin 位置 1 的 continuation 结束
- 分片中途出现新的数据帧 → `ErrNewDataFrameInFragment`（分片类型不一致
  同属此类，因为 continuation 不携带类型）
- 无在途分片却收到 continuation → `ErrContinuationWithoutStart`
- 控制帧可插入分片序列中间，立即透传且不干扰在途消息
- 重组结果与未分片发送逐字节一致；累计大小超过 `MaxMessageSize` 拒绝
  （`ErrMessageTooLarge`），拒绝后重组器自动复位可继续服役

### 关闭握手与关闭原因（conn.go, close.go）

`Close()` 发送关闭帧并等待对端回应（`CloseTimeout` 上限），幂等且并发安全：
重复或并发调用只生效一次，所有调用者观察到同一个 `*CloseError`。

连接终止原因通过 `Conn.CloseReason()` 与 `OnClose` 回调观测，彼此可区分：

| CloseReason | 含义 | 典型关闭码 |
|---|---|---|
| `local-close` | 本地调用 `Close()` 并完成握手 | 1000 |
| `peer-close` | 对端先发关闭帧，本端回应 | 对端携带的码 |
| `heartbeat-timeout` | 心跳连续缺失达阈值 | 1001 |
| `read-error` | 读侧 I/O 或协议错误 | 1002/1006/1009 |
| `write-error` | 写侧 I/O 失败 | 1006 |
| `send-queue-full` | 发送队列满且 `SendTimeout` 超时 | 1008 |
| `extension-rejected` | 未协商压缩却收到 RSV1 帧 | 1002 |

竞态规则：本地已确定的原因（如本地关闭、心跳超时）不会被随后到达的
对端关闭帧覆盖。

### 心跳保活（heartbeat.go）

每个 `Interval` 发送一次 ping（负载为递增序号，便于对账）；`Timeout` 内
未收到 pong 记一次缺失，任何入站数据都会重置缺失计数（对端发数据即证明
存活）。连续缺失达到 `MaxMisses` 后以 `heartbeat-timeout` 终止连接。

注意：若已有未应答的 ping，新的 ping 不会重置其截止时间，因此
`Interval < Timeout` 的配置也能正确检出死亡对端。

### 压缩扩展（compress.go）

实现 RFC 7692 permessage-deflate，双方向均保持压缩上下文
（context takeover）：flate 字典跨消息保留，解压侧以最近 32 KiB 明文作为
字典。协商（`Negotiator`）：

- `Offer()` 生成 `Sec-WebSocket-Extensions: permessage-deflate`
- `Accept()` 校验对端头部；`*_no_context_takeover`、`*_max_window_bits`
  及未知参数一律拒绝（`ErrUnsupportedExtensionParam`）
- 本地未启用而对端头部包含该扩展 → `ErrExtensionNotNegotiated`
- 连接层收到 RSV1 帧但 `CompressionActive` 为假 → 以
  `extension-rejected` 终止

压缩只应用于 ≥ `Compress.MinSize` 的数据消息；收发两个方向使用同一协商
结果，行为对称。解压输出受 `MaxMessageSize` 约束，防止压缩炸弹耗尽内存。

### 并发与背压（conn.go）

- 读循环与写循环分离；`Send`/`SendFragmented` 可被任意数量 goroutine 调用
- 一个逻辑消息（含其全部分片）作为整体入队，由唯一写协程顺序写出，
  任何时刻不会出现帧内容交叉或半帧写出；压缩在写循环内执行，保证字典
  顺序与线序一致
- 发送队列有界（`SendQueueSize`）。队列满时：`SendTimeout == 0` 阻塞至
  有空间或连接关闭；`SendTimeout > 0` 超时后该次发送失败
  （`ErrSendTimeout`）并以 `send-queue-full` 终止连接
- 关闭与超时期间：未完成的 `Send`/`ReadMessage` 立即以 `*CloseError`
  返回；队列中未写出的消息被丢弃收敛；终止时的尽力关闭帧不会在写路径
  卡死时阻塞清理（TryLock 跳过）

## 配置项（Config）

| 字段 | 默认 | 说明 |
|---|---|---|
| `MaxFramePayload` | 1 MiB | 单帧负载上限 |
| `MaxMessageSize` | 16 MiB | 重组消息 / 解压输出上限 |
| `SendQueueSize` | 64 | 发送队列容量（消息条数） |
| `SendTimeout` | 0（无限阻塞） | 队列满时 Send 的等待上限 |
| `WriteTimeout` | 10 s | 单次 socket 写截止 |
| `CloseTimeout` | 5 s | 等待对端关闭回应的上限 |
| `Heartbeat` | nil（关闭） | 心跳配置，见上 |
| `Compress` | nil（关闭） | 压缩配置，见上 |
| `IsServer` | false | 角色：服务端要求入站帧掩码、出站不掩码 |
| `CompressionActive` | false | 握手层协商结果，双方向一致生效 |

## 资源占用与性能上界

- 解析缓冲区：≤ `MaxFramePayload + 14` 字节，与消息总量无关
- 重组缓冲区：≤ `MaxMessageSize`；超限即拒绝并复位
- 解压输出：≤ `MaxMessageSize`（硬上限 1 GiB），超限拒绝
- 发送侧内存：≤ `SendQueueSize` 条消息 + 一条在写消息
- 压缩字典：固定 32 KiB 滑窗

基准（Xeon Platinum 8255C，`go test -bench . -benchmem`）：

| 路径 | 吞吐 | 分配 |
|---|---|---|
| 解析 1 MiB 帧 | ~1.25 GB/s | 3 allocs/op |
| 解析 64 B 帧流 | ~0.4 GB/s | ~2 allocs/帧 |
| 序列化 4 KiB 帧 | ~1.5 GB/s | 1 alloc/op |
| 压缩 16 KiB（稳态） | ~280 MB/s | 1 alloc/op |
| 解压 16 KiB | ~237 MB/s | 有界（输出尺寸） |

解析与序列化均为单次拷贝、O(n) 时间；分配次数与消息量无关（稳态常数）。

## 测试

```sh
go test ./...          # 单元 + 集成测试
go test ./... -race    # 竞态检测
go test -bench . -benchmem ./ws/
```

覆盖：三种长度表示与任意截断解析、分片重组与非法序列拒绝、控制帧约束、
双向/并发/幂等关闭握手、心跳超时与健康保活、压缩协商与拒绝、端到端压缩、
多生产者并发发送无交叉、背压与发送超时、超大消息拒绝、掩码规则强制。
