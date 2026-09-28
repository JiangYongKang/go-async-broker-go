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

所有状态文件位于 `Config.Dir`：`state.json`（消息、位点与重放序号）、
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

两者都返回 `ReplayResult{ReplayID, Group, Total, Replayed, Skipped, Remaining}`。
返回计数与实际状态变化严格一致：`Replayed` 只统计真正从 `dead` 置回
`pending` 的消息，`Skipped` 是目标中当前已不是死信的消息，二者互不相混。

**重放语义与兼容性**

- **整批原子（all-or-nothing）**：一次调用只可能有两种确定结果——
  放得下就整批重放成功（`error=nil`，`Replayed=Total-Skipped`、
  `Remaining=0`）；放不下就整批失败、什么都不发生。不会出现"返回成功
  却只放进去一部分，剩下的由后台偷偷续跑"的情况，因此不存在未完成
  重放意图，也没有后台续跑泵。
- **容量策略与普通生产完全对齐**：死信不占用容量名额，但重放消息
  （`dead -> pending`）计入 `MaxPending`（pending+inflight），判定以
  整批为单位（当前占用 + 死信目标数）：
  - `PolicyReject`：整批放不下时**立即返回与 `Produce` 相同的
    `ErrBrokerFull`**，不消费序号、不消费重放 ID、不改动任何消息，
    死信的状态、序号、组内顺序、`DeadReason`/`ErrorClass`/`LastError`
    与历史轨迹全部原样保留。调用方按与生产背压完全相同的方式处理这个
    错误（稍后整批重试或降级），不需要猜测"重放了几条、还剩几条"。
  - `PolicyBlock`：阻塞等待，直到容量**能一次性容纳整批**才继续，
    最终整批完成；部分名额释放不会触发部分重放。等待期间 `ctx` 取消
    返回 `ctx.Err()` 且整批不生效；Broker 关闭返回 `ErrClosed`。
  - 两条策略下任何时刻占用都不会超过 `MaxPending`。被拒绝后等容量
    释放再重放，结果与"第一次容量就充足"完全一致。
- **同一条消息原地复活**：重放不是复制新消息，而是把同一条记录从
  `dead` 置回 `pending`、把本轮 `Attempts` 清零（重新获得完整的
  `MaxRetries` 重试机会）、清除退避时间，并按旧序号顺序连续分配新的
  全局单调序号，整批排到所属分组队尾。之后它走与正常消息完全一致的
  投递、确认、重试、再次进死信流程。
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
- **重复调用 / 并发安全**：重放在持锁下"冻结目标 + 容量判定 + 整批
  落盘"一气呵成。重复或并发重放同一条死信时，只有一个调用真正生效，
  其余发现目标已不是 `dead`，计入 `Skipped`（全部命中时返回
  `ErrEmptyReplay`），任何情况下都不会产生两条在途消息、不丢消息、
  不打乱组内顺序。配合消费端 `Dedup` 的持久化幂等，重复投递不会造成
  重复计费或重复副作用。
- **不破坏组内顺序**：重放消息分配的是队尾新序号；并且投递规则为
  严格队首——同一分组里只要还有更小序号的 pending 消息（即使它正在
  退避等待重试），后面的消息（含重放消息）就不会被投递。因此重放消息
  不会插到同组未处理完的消息前面；同一批内多条死信按其旧序号顺序
  连续分配新序号，重放后相对顺序与进死信的顺序一致。
- **落盘原子、崩溃结果确定**：整批的所有状态变更（含序号计数器）在
  **同一个原子快照**（tmp + fsync + rename）内落盘；落盘失败时整批
  回滚到调用前状态，调用方拿到失败即等价于什么都没发生。因此不存在
  "半批"状态：重放成功后进程退出，重启后整批都在；重放被拒绝则磁盘
  上没有任何痕迹。和既有恢复规则一致，重放后处于 inflight 的消息
  重启时也回到 pending 重新投递（至少一次，由 `Dedup` 兜底副作用）。

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
- 死信重放复用同一套判定与错误：拒绝策略下整批放不下时同样立即返回
  `ErrBrokerFull`（详见上文"死信重放"），调用方可用同一套统一背压
  处理逻辑覆盖生产与重放两类入口。
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
拒绝策略容量不足时整批被拒绝且无后台续跑、容量释放后重放正常完成、
阻塞策略等到整批放得下才继续（含 ctx 取消整批不生效）、
容量边界上并发重放恰好一次且全程不超过上限、重放整批原子落盘重启后
恰好一次、重放后再次耗尽原因可区分）。

重放容量边界可在本地复现与验证（与
`TestReplayRejectAtCapacityWholeBatchAborted` 等用例一致）：

1. 用较小 `MaxPending`（如 4）+ `PolicyReject` 打开 Broker，先制造
   若干死信（死信不占名额），再 `Receive` 占用 3 个容量名额；
2. 调 `ReplayMessages(ctx, 3条死信)`：必然立即拿到
   `errors.Is(err, ErrBrokerFull)`，且 `DeadLetters()` 的数量/顺序、
   每条的 `Status/Seq/Round/DeadReason/DeadHistory/ReplayHistory`
   与调用前逐项相同，`Pending()==3`，等待后也无任何消息被投递；
3. `Ack` 释放名额后再次整批重放：成功且
   `Replayed==3 && Remaining==0 && ReplayID=="replay-1"`（被拒绝的
   尝试不消费重放 ID），三条按进死信顺序各投递一次，`Pending()` 全程
   不超过 `MaxPending`；把 `Policy` 换成 `PolicyBlock` 重复上述步骤，
   可观察到调用阻塞到整批放得下后才一次性返回。
