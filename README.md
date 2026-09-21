# go-async-broker-go

一个可在本地运行的异步消息投递与任务处理引擎。用本地文件存储模拟消息
代理与消费状态，使消息在生产、投递、重试、死信与重启恢复各环节的结果
一致、可解释、可验证。

## 结构

```
broker/
  types.go    消息模型、状态机、可区分错误、配置
  store.go    状态快照的原子持久化（tmp + fsync + rename）与恢复
  broker.go   引擎主体：生产、按组有序投递、确认/拒绝、重试退避、死信、背压
  offset.go   消费位点提交（严格单调，回退拒绝）
  outbox.go   事务性发件箱：业务写入与待发消息的原子提交与崩溃重放
  dedup.go    消费端幂等器：抑制重复投递造成的重复副作用
```

所有状态文件位于 `Config.Dir`：`state.json`（消息与位点）、
`outbox.json`（提交意图日志）、`biz.json`（模拟业务库）、
`dedup.json`（消费端去重记录）。

## 投递语义

- **至少一次**：消息先落盘再对外可见；已投递未确认（inflight）的消息在
  进程重启后一律回到 pending 重新投递，不会丢失。
- **组内有序**：同一 `Group` 同一时刻至多一条 inflight，且只允许组内最
  小序号投递；确认或重试完成前，同组后续消息不会发出，因此正常投递与
  重试过程中组内顺序稳定，不受并发消费影响。
- **确认/拒绝的确定性**：
  - `Ack` 未知消息 → `ErrUnknownMessage`
  - `Ack` 已确认消息（重复确认）→ `ErrAlreadyAcked`
  - `Ack` 其他非 inflight 状态 → `ErrNotInflight`（携带当前状态）
  - 三类原因可用 `errors.Is` 区分；失败不会改变消息现有状态。
- **消费端幂等**：至少一次语义下重复投递不可避免，`Dedup.ProcessOnce`
  以持久化记录保证同一消息的副作用（如计费）只生效一次，重复投递返回
  `ErrDuplicate`。

## 重试与死信规则

- `Nack(id, retryable, cause)`：
  - `retryable=true`：按指数退避重投，第 n 次等待
    `min(BackoffBase << (n-1), BackoffMax)`；当已投递次数超过
    `MaxRetries` 时进入死信，判定依据 `DeadReason=retry_exhausted`。
  - `retryable=false`：不消耗重试次数，直接进入死信，判定依据
    `DeadReason=non_retryable`。
- 每次判定都记录在消息的 `ErrorClass` / `DeadReason` / `LastError`
  字段上，可通过 `Get` / `DeadLetters` 观测；死信消息保留在存储中，
  不会被静默丢弃。

## 原子提交与恢复（事务性发件箱）

`Outbox.AtomicCommit(opID, bizOps, msgs...)` 按以下顺序执行，每步幂等：

1. 意图（业务写入 + 待发消息）写入 `outbox.json` 并落盘；
2. 应用业务写入到 `biz.json`（按 opID 去重）；
3. 消息入队（按调用方给定的确定性消息 ID 去重）；
4. 标记意图完成并落盘。

进程在任意两步之间崩溃后，`Outbox.Recover()` 按相同顺序重放未完成
意图：已应用的业务不会重复应用，已入队的消息不会重复入队。因此不会
出现"业务已提交却丢消息"，也不会出现"消息可见但业务未提交"。
业务写入建模为可序列化的 `BizOp`（KV 写入），这是本地模拟的要求；
接入真实数据库时，等价做法是把意图与业务写入放进同一事务。

## 背压与容量上限

- `MaxPending` 限制 pending + inflight 总量，内存占用有界。
- 达到上限时按 `Policy` 行为：`PolicyBlock` 阻塞生产者直到容量释放
  （或 ctx 取消）；`PolicyReject` 立即返回 `ErrBrokerFull`。
- 两种行为都确定且可观测（`Pending()`），已接收的消息不受影响、不丢失。
- 已提交事务（outbox）的消息不受上限约束，保证已提交事务不丢消息。

## 消费位点

- `CommitOffset(group, offset)` 只接受严格单调前进的位点；回退或与已
  提交值相同（崩溃恢复后的重复提交）都返回 `ErrOffsetRegression`，
  不会从中间继续或重复提交。位点与消息状态同事务落盘，重启后不丢。

## 本地验证

```bash
go build ./...
go test ./broker/ -v        # 全部单测，日志打印输入与判定依据
go test ./broker/ -race     # 并发一致性（多生产者/消费者/中途恢复）
go run ./cmd/demo           # 端到端演示：原子提交/顺序/重试/死信/崩溃恢复
```

测试以失败与边界链路为主，覆盖：重复投递与消费端幂等、重试耗尽进死信、
不可重试错误直接进死信、确认/拒绝的三类可区分错误、位点回退拒绝、
背压阻塞与拒绝、崩溃恢复（inflight 重投、outbox 三种崩溃点重放、
损坏状态文件明确报错）、并发下组内顺序稳定与恰好一次计费。
