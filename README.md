# 本地异步消息投递与任务处理引擎

这是一个零外部中间件依赖的 Go 本地消息代理实现。消息、消费组状态、重试状态、死信和业务 KV 都写入本地目录中的持久化 WAL（write-ahead log），用于模拟可重启、可验证的消息代理。

## 包入口

```go
b, err := broker.Open(ctx, "./data", broker.Config{...})
defer b.Close(ctx)

err = b.Subscribe(ctx, "orders", "billing", func(ctx context.Context, d *broker.Delivery) broker.HandlerResult {
    // BusinessWrites 与 Ack 在同一个本地事务中提交。
    return broker.AckResult("invoice created", map[string][]byte{
        "invoice:" + d.Message.ID: []byte("done"),
    })
})

_, err = b.Publish(ctx, broker.PublishInput{
    Topic: "orders",
    Key:   "customer-123",
    Body:  []byte(`{"amount":100}`),
    BusinessWrites: map[string][]byte{
        "outbox:order-123": []byte("prepared"),
    },
})
```

## 投递语义

- **至少一次投递**：消息在投递给消费者前先持久化交付租约；若进程在消费者返回前崩溃，重启后同一条消息会再次投递。
- **消费端无重复本地副作用**：消费者通过 `AckResult(..., BusinessWrites)` 把本地业务写入和确认放入同一个 WAL 批次提交。重复投递再次确认会被拒绝，错误码为 `ALREADY_ACKED`。
- **未知和过期确认可区分**：
  - `UNKNOWN_DELIVERY`：交付令牌不属于任何未完成或最近完成的交付；
  - `ALREADY_ACKED`：同一交付已经确认；
  - `ALREADY_DEAD_LETTERED`：同一交付已经死信；
  - `STALE_DELIVERY_ATTEMPT`：消息仍未终态，但该交付 ID 已被更新的租约取代。
- **终态确定**：Ack 或 Nack 只有在 WAL 批次 fsync 成功后才更新内存状态；提交成功后的重复提交不会再次应用业务写入。

> 当前实现保证的是本地 KV 副作用与消息终态原子。对 HTTP、邮件、外部数据库等外部副作用，仍需业务侧使用消息 ID 做幂等键；消息代理无法替外部系统回滚。

## 分组键与顺序

同一个 `(topic, group, key)` 内严格串行：

1. 队首消息未确认、等待重试或处于处理中时，同键后续消息不会开始；
2. 可重试失败按退避时间重试；
3. 不同 key 可以并发；
4. 每个 key 维护独立的、从 1 开始的严格递增位点。

消息还有一个 topic 内全局 `Offset` 用于审计，但不同 key 的全局 offset 天然不连续；消费组顺序判断使用 per-key sequence。恢复时如果日志中的终态位点回退或跳位，会返回 `OFFSET_ROLLBACK`，不会从中间继续或跳过消息。

## 重试与死信

消费者显式返回：

```go
return broker.Retry("temporary dependency unavailable", err)
return broker.DeadLetter("invalid payload schema", err)
return broker.AckResult("processed", businessWrites)
```

规则如下：

- `DecisionRetry`：按 topic 的 `Backoff` 计算等待时间，重试次数从 1 开始；
- `DecisionDeadLetter`：不可重试错误立即进入死信，不再消耗后续尝试；
- 可重试错误达到 `MaxAttempts` 后进入死信，原因标记为重试耗尽；
- 死信不会静默丢弃，原始消息、key、尝试次数、业务原因、最后错误和时间均可通过 `DeadLetters(topic, group)` 读取；
- 死信消息继续占用容量，直到运维或应用调用 `DeleteDeadLetter(ctx, topic, group, messageID)` 显式清理，避免 poison message 被无限接收后又悄悄消失。

死信原因包含稳定前缀，可区分：

- `non-retryable:`：消费者明确判定不可重试；
- `retry exhausted:`：每一次重试都失败并达到上限。

## 原子提交与重启恢复

### 生产端

`Publish` 在一个 WAL 批次中写入：

- 消息内容、key、headers 和全局 offset；
- 待投递的消费组引用；
- `BusinessWrites`。

因此只有两种结果：

- 批次完整提交：业务写入与消息同时可见；
- 批次未完整提交：恢复时忽略撕裂尾部，业务写入和消息都不可见。

不会出现“业务已提交但消息丢失”或“消息可见但业务未提交”。

### 消费端

Ack/DeadLetter 与可选业务写入在同一批次提交。崩溃恢复规则：

- `pending`：继续等待派发；
- `retry_wait`：保留持久化的 `visible_at`，不会提前投递；
- `inflight`：说明消费者终态未提交，重启后立即重新置为可见；
- 已终态：位点已提交，不重复投递。

WAL 使用 `begin/data.../end` 批次帧、CRC32 校验、长度前缀和目录 fsync。只有完整 end 帧的批次可见；撕裂尾部会在打开时截断。校验和错误不会被当作普通崩溃忽略，会返回 `CORRUPT_STORAGE`。

## 背压与容量

`TopicConfig.MaxCapacity` 限制 topic 当前保留记录数：

- 未完成消息占容量；
- 死信消息也占容量；
- Ack 且无其他消费组引用后释放容量。

`PublishInput.NonBlocking=true` 时，容量已满立即返回 `CAPACITY_EXCEEDED`。默认阻塞发布在终态提交释放容量后继续，且等待可通过 context 取消。容量状态可用 `Stats(topic)` 观察。

## 本地存储和并发模型

- 每个存储目录有文件锁；同一时刻只允许一个 Broker 进程打开。
- 一个 Broker 进程内支持多个生产者 goroutine、多个不同 key 的消费者 goroutine 并发运行。
- 状态更新在同一把 Broker 锁内完成，并先持久化后修改内存状态，避免半更新。
- WAL 当前为追加日志；本项目规模下测试和本地验证足够。长期运行的生产系统应加入 checkpoint/snapshot 和日志压缩。

## 配置

```go
cfg := broker.Config{
    DefaultCapacity: 1024,
    DefaultAttempts: 5,
    DefaultBackoff: broker.ExponentialBackoff{
        Initial: 20 * time.Millisecond,
        Factor:  2,
        Maximum: time.Second,
    },
    Topics: map[string]broker.TopicConfig{
        "orders": {
            MaxCapacity: 10000,
            MaxAttempts: 8,
        },
    },
}
```

`MaxCapacity: -1` 表示不限制容量；不建议在真实长时间运行进程中使用。

## 本地验证

```sh
go test ./...
go test -race ./...
go vet ./...
```

测试重点覆盖：

- 正常生产、消费、Ack 与重启；
- 同 key 在普通投递和重试中不错序；
- 重复确认、未知确认、过期交付确认；
- 可重试错误耗尽与不可重试错误立即死信；
- 生产/消费业务写入原子提交；
- WAL 撕裂尾部恢复；
- inflight 与 retry-wait 状态重启；
- per-key 位点回退和跳位拒绝；
- 非阻塞/阻塞背压；
- 多生产者、多 key 消费者并发和 race 检测。
