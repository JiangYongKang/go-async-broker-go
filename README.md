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
  replay.go   死信重放：按消息/按分组把死信送回正常投递链路
```

所有状态文件位于 `Config.Dir`：`state.json`（消息、位点与未完成重放意图）、
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

## 死信重放

根因修复后，可以把死信重新送回正常投递链路再消费一遍。两个入口：

- `ReplayMessages(ctx, ids)`：按一批消息 ID 重放；未知 ID 返回
  `ErrUnknownMessage` 且整批不生效。
- `ReplayGroup(ctx, group)`：按分组重放该组当前全部死信。

两者都返回 `ReplayResult{ReplayID, Total, Replayed, Skipped, Remaining}`。

**重放语义与兼容性**

- **同一条消息原地复活**：重放不是复制新消息，而是把同一条记录从
  `dead` 置回 `pending`、把本轮 `Attempts` 清零（重新获得完整的
  `MaxRetries` 重试机会）、清除退避时间，并分配一个新的全局单调序号，
  排到所属分组的队尾。之后它走与正常消息完全一致的投递、确认、重试、
  再次进死信流程。
- **历史不抹除、可解释**：每次进死信都会在 `Message.DeadHistory`
  追加一条不可变 `DeadEvent`（轮次、原因、错误分类、cause、投递次数、
  时间），每次重放都在 `Message.ReplayHistory` 追加 `ReplayEvent`
  （轮次、重放请求 ID、旧序号→新序号、时间）。重放只清空"当前状态字段"
  （`DeadReason/ErrorClass/LastError`），既有判定记录全部保留，可通过
  `Get` 逐条核对重放前后状态。`Round` 字段标记当前处于第几轮（0 为首次，
  ≥1 为第 N 次重放）。
- **首次失败 vs 重放后再失败可区分**：重放后再次耗尽重试，判定原因为
  `replay_retry_exhausted`；重放后再遇不可重试错误，原因为
  `replay_non_retryable`，与首次的 `retry_exhausted` /
  `non_retryable` 明确区分。重放后若 Ack 成功，则正常结束。
- **重复调用 / 并发安全**：只有当前处于 `dead` 的消息会被冻结进重放请求；
  已复活的消息被重复或并发重放时计入 `Skipped`（全部命中时返回
  `ErrEmptyReplay`），不会产生第二条在途消息。配合消费端 `Dedup`
  的持久化幂等，重复投递不会造成重复计费或重复副作用。
- **不破坏组内顺序**：重放消息分配的是队尾新序号；并且投递规则强化为
  严格队首——同一分组里只要还有更小序号的 pending 消息（即使它正在
  退避等待重试），后面的消息（含重放消息）就不会被投递。因此重放消息
  不会插到同组未处理完的消息前面。
- **受容量上限约束**：重放同样计入 `MaxPending`（pending+inflight），
  不会绕过上限导致内存无界增长。容量不足时按既有策略处理：
  `PolicyBlock` 调用阻塞直到全部重放完成（或 ctx 取消）；
  `PolicyReject` 先重放容量内的一批，其余目标持久化为未完成重放意图，
  由后台泵在每次容量释放时分块续跑，调用方可立即从 `Remaining` 与
  `PendingReplays()` 观测进度。
- **崩溃恢复结果确定**：重放意图（冻结的目标 ID 列表 + 每条的 Applied
  标记）与首批状态变更在**同一个原子快照**内落盘。重放推进到一半进程
  退出，重启后后台泵按冻结目标幂等续跑：已 Applied 的不会重放第二次，
  未 Applied 的不会丢。和既有恢复规则一致，重放后处于 inflight 的消息
  重启时也回到 pending 重新投递（至少一次，由 `Dedup` 兜底副作用）。
- 未完成的重放意图可通过 `PendingReplays()` 查询（ID、分组、冻结目标、
  已应用集合）；完成后自动清除。

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
go test ./broker/ -race     # 并发一致性（多生产者/消费者/中途恢复/重放）
go run ./cmd/demo           # 端到端演示：原子提交/顺序/重试/死信/重放/崩溃恢复
```

测试以失败与边界链路为主，覆盖：重复投递与消费端幂等、重试耗尽进死信、
不可重试错误直接进死信、确认/拒绝的三类可区分错误、位点回退拒绝、
背压阻塞与拒绝、崩溃恢复（inflight 重投、outbox 三种崩溃点重放、
损坏状态文件明确报错）、并发下组内顺序稳定与恰好一次计费，以及
死信重放主链路与边界（重复/并发重放幂等、与同组退避消息的顺序、
容量受限分块续跑、重放中途重启恰好一次、重放后再次耗尽原因可区分）。
