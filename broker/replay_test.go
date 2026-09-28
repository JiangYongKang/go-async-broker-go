package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// kill 把一条已生产消息送入死信（不可重试，立即死亡）。
func kill(t *testing.T, b *Broker, id string) {
	t.Helper()
	m := recvCtx(t, b)
	if m.ID != id {
		t.Fatalf("want %s, got %s", id, m.ID)
	}
	if err := b.Nack(id, false, "poison"); err != nil {
		t.Fatal(err)
	}
}

// drainReceive 在 timeout 内接收一条消息，失败返回 nil。
func drainReceive(b *Broker, timeout time.Duration) *Message {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	m, err := b.Receive(ctx)
	if err != nil {
		return nil
	}
	return m
}

// 主链路：死信 -> 按消息重放 -> 重新投递 -> 确认；
// 原死信记录与每轮判定原因保留，可本地核对。
func TestReplayMessagesMainPath(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g1", []byte("x"))
	kill(t, b, id)
	before, _ := b.Get(id)
	t.Logf("输入: %s 已进死信 reason=%s round=%d", id, before.DeadReason, before.Round)

	res, err := b.ReplayMessages(context.Background(), []string{id})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("判定依据: 重放结果 total=%d replayed=%d remaining=%d replayID=%s",
		res.Total, res.Replayed, res.Remaining, res.ReplayID)
	if res.Replayed != 1 || res.Remaining != 0 {
		t.Fatalf("unexpected replay result: %+v", res)
	}

	got, _ := b.Get(id)
	if got.Status != StatusPending && got.Status != StatusInflight {
		t.Fatalf("want back in delivery, got %s", got.Status)
	}
	if got.Round != 1 || got.Attempts != 0 {
		t.Fatalf("round/attempts not reset: round=%d attempts=%d", got.Round, got.Attempts)
	}
	if len(got.DeadHistory) != 1 || got.DeadHistory[0].Reason != DeadNonRetryable {
		t.Fatalf("original dead record lost: %+v", got.DeadHistory)
	}
	if len(got.ReplayHistory) != 1 || got.ReplayHistory[0].Round != 1 ||
		got.ReplayHistory[0].FromSeq == 0 || got.ReplayHistory[0].ToSeq <= got.ReplayHistory[0].FromSeq {
		t.Fatalf("replay record broken: %+v", got.ReplayHistory)
	}
	t.Logf("判定依据: 死信历史保留=%+v，重放历史=%+v", got.DeadHistory, got.ReplayHistory)

	m := recvCtx(t, b)
	if m.ID != id {
		t.Fatalf("want redelivery %s, got %s", id, m.ID)
	}
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}
	done, _ := b.Get(id)
	if done.Status != StatusAcked {
		t.Fatalf("want acked after replay, got %s", done.Status)
	}
	if len(b.DeadLetters()) != 0 {
		t.Fatal("replayed message must leave dead letter list")
	}
	t.Log("判定依据: 重放消息像正常消息一样被投递并确认，死信列表相应移出")
}

// 重复/并发重放幂等：同一条死信被重放多次不会产生多条在途消息。
func TestReplayIdempotentAndConcurrent(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g1", []byte("x"))
	kill(t, b, id)

	// 第一次重放成功。
	if _, err := b.ReplayMessages(context.Background(), []string{id}); err != nil {
		t.Fatal(err)
	}
	// 立即重复调用：已是 pending，ErrEmptyReplay 且无新增在途。
	if _, err := b.ReplayMessages(context.Background(), []string{id, id}); !errors.Is(err, ErrEmptyReplay) {
		t.Fatalf("duplicate replay: want ErrEmptyReplay, got %v", err)
	}
	if b.Pending() != 1 {
		t.Fatalf("duplicate replay created in-flight duplicates: Pending=%d", b.Pending())
	}
	t.Logf("判定依据: 串行重复重放被抑制，Pending=%d（仍为 1）", b.Pending())
	// 先消费掉已重放的 id，避免它干扰后续投递计数。
	if m := drainReceive(b, time.Second); m == nil || m.ID != id {
		t.Fatalf("want replayed %s, got %v", id, m)
	} else if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}

	// 再制造一条死信，多消费者并发重放同一条：恰好一个成功。
	id2, _ := b.Produce(context.Background(), "g2", []byte("y"))
	kill(t, b, id2)
	var wg sync.WaitGroup
	var okN, emptyN int
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := b.ReplayMessages(context.Background(), []string{id2})
			mu.Lock()
			switch {
			case err == nil:
				okN++
			case errors.Is(err, ErrEmptyReplay):
				emptyN++
			default:
				t.Errorf("concurrent replay: %v", err)
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if okN != 1 || emptyN != 7 {
		t.Fatalf("concurrent replay: ok=%d empty=%d, want 1/7", okN, emptyN)
	}
	if b.Pending() != 1 {
		t.Fatalf("concurrent replay duplicated messages: Pending=%d", b.Pending())
	}
	// 实际接收去重：id2 只会被投递一次。
	seen := map[string]int{}
	m := drainReceive(b, time.Second)
	if m == nil {
		t.Fatal("missing delivery")
	}
	seen[m.ID]++
	if seen[id2] != 1 {
		t.Fatalf("delivery not exactly-once: %v", seen)
	}
	if err := b.Ack(id2); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定依据: 8 并发重放仅 1 个生效，投递计数 %v（无重复在途）", seen)
}

// 按分组重放；且重放消息不能越过组内尚未处理完（含退避等待）的消息。
func TestReplayGroupOrderingBehindBackoff(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxRetries = 2
	cfg.BackoffBase = 400 * time.Millisecond
	cfg.BackoffMax = time.Second
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// D 先死；A 后生产并进入退避。重放 D 后，D 必须排在 A 后面。
	d, _ := b.Produce(context.Background(), "g", []byte("dead"))
	kill(t, b, d)
	a, _ := b.Produce(context.Background(), "g", []byte("alive-backoff"))
	m := recvCtx(t, b)
	if m.ID != a {
		t.Fatalf("want %s, got %s", a, m.ID)
	}
	if err := b.Nack(a, true, "flaky"); err != nil { // A 进入退避
		t.Fatal(err)
	}
	res, err := b.ReplayGroup(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 1 || res.Replayed != 1 {
		t.Fatalf("replay group: %+v", res)
	}
	t.Logf("输入: 组 g 中 A=%s 退避中，D=%s 重放回来（新序号排在队尾）", a, d)

	// A 仍在退避时，即便 D 已就绪也不得投递（不能插队）。
	if got := drainReceive(b, 150*time.Millisecond); got != nil {
		t.Fatalf("replayed message jumped the queue: %s", got.ID)
	}
	t.Log("判定依据: A 退避未到期，重放的 D 被队首阻塞，没有插到前面")

	first := recvCtx(t, b)
	if first.ID != a {
		t.Fatalf("group order broken: want A %s, got %s", a, first.ID)
	}
	if err := b.Ack(a); err != nil {
		t.Fatal(err)
	}
	second := recvCtx(t, b)
	if second.ID != d {
		t.Fatalf("want replayed D %s after A, got %s", d, second.ID)
	}
	t.Log("判定依据: 退避到期后投递顺序 A -> D（重放消息遵守组内顺序）")
	if err := b.Ack(d); err != nil {
		t.Fatal(err)
	}

	// 空分组重放 -> ErrEmptyReplay。
	if _, err := b.ReplayGroup(context.Background(), "nope"); !errors.Is(err, ErrEmptyReplay) {
		t.Fatalf("want ErrEmptyReplay, got %v", err)
	}
}

// deadSnapshot 记录死信在重放前后必须严格保持不变的字段。
type deadSnapshot struct {
	status     Status
	seq        uint64
	round      int
	attempts   int
	deadReason DeadReason
	errorClass ErrorClass
	lastError  string
	deadN      int
	replayN    int
}

func deadSnapshotOf(t *testing.T, b *Broker, id string) deadSnapshot {
	t.Helper()
	m, err := b.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return deadSnapshot{
		status: m.Status, seq: m.Seq, round: m.Round, attempts: m.Attempts,
		deadReason: m.DeadReason, errorClass: m.ErrorClass, lastError: m.LastError,
		deadN: len(m.DeadHistory), replayN: len(m.ReplayHistory),
	}
}

// inflight 生产一条不同分组的消息并立即接收，使其占用一个容量名额。
func inflight(t *testing.T, b *Broker, group string) string {
	t.Helper()
	id, err := b.Produce(context.Background(), group, []byte("occupy"))
	if err != nil {
		t.Fatal(err)
	}
	m := recvCtx(t, b)
	if m.ID != id {
		t.Fatalf("want %s, got %s", id, m.ID)
	}
	return id
}

// 容量（拒绝策略）：整批放不下时立刻返回与 Produce 相同的 ErrBrokerFull，
// 整批不生效——死信状态/序号/顺序/原因不变、没有后台续跑、不消费重放 ID；
// 容量释放后整批重放成功，结果与第一次就成功一致，再次进死信原因可区分。
func TestReplayRejectAtCapacityWholeBatchAborted(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 4
	cfg.Policy = PolicyReject
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()

	var dead []string
	for i := 0; i < 3; i++ {
		id, _ := b.Produce(ctx, "gX", []byte("d"))
		kill(t, b, id)
		dead = append(dead, id)
	}
	before := map[string]deadSnapshot{}
	for _, id := range dead {
		before[id] = deadSnapshotOf(t, b, id)
	}

	// 占用 2/4 个名额（不同分组，均 inflight），仅剩 2 个名额 < 整批 3 条：
	// 即使其中 2 条放得下，也必须整批拒绝，绝不允许部分放入。
	holders := []string{
		inflight(t, b, "gZ1"),
		inflight(t, b, "gZ2"),
	}

	res, err := b.ReplayMessages(ctx, dead)
	if !errors.Is(err, ErrBrokerFull) {
		t.Fatalf("want ErrBrokerFull（与 Produce 同一背压错误）, got res=%+v err=%v", res, err)
	}
	if res != nil {
		t.Fatalf("被拒绝的重放不得返回成功类结果: %+v", res)
	}
	t.Log("判定依据: 容量不足时重放立即返回 ErrBrokerFull，由调用方决定稍后重试或降级")

	if b.Pending() != 2 {
		t.Fatalf("拒绝后容量占用被改变: Pending=%d", b.Pending())
	}
	// 死信列表数量、顺序、每条的状态/序号/轮次/原因/历史一律不变。
	dls := b.DeadLetters()
	if len(dls) != len(dead) {
		t.Fatalf("dead letter count changed: %d", len(dls))
	}
	for i, id := range dead {
		if dls[i].ID != id {
			t.Fatalf("死信顺序在 %d 处变化: want %s got %s", i, id, dls[i].ID)
		}
		if got := deadSnapshotOf(t, b, id); got != before[id] {
			t.Fatalf("死信 %s 被拒绝重放改动: before=%+v after=%+v", id, before[id], got)
		}
	}
	// 不存在后台续跑：等一段时间，不应有任何 gX 消息被投递，状态仍不变。
	if m := drainReceive(b, 150*time.Millisecond); m != nil {
		t.Fatalf("拒绝后出现后台续跑投递: %s", m.ID)
	}
	for _, id := range dead {
		if got := deadSnapshotOf(t, b, id); got != before[id] {
			t.Fatalf("等待期间死信 %s 被改动: %+v", id, got)
		}
	}
	t.Log("判定依据: 整批被拒绝后死信状态/序号/顺序/原因不变，且无后台续跑")

	// 释放容量后整批重放：结果与第一次容量充足时成功完全一致。
	for _, id := range holders {
		if err := b.Ack(id); err != nil {
			t.Fatal(err)
		}
	}
	res2, err := b.ReplayMessages(ctx, dead)
	if err != nil {
		t.Fatalf("容量释放后重放应成功: %v", err)
	}
	if res2.Total != 3 || res2.Replayed != 3 || res2.Skipped != 0 || res2.Remaining != 0 {
		t.Fatalf("unexpected replay result: %+v", res2)
	}
	if res2.ReplayID != "replay-1" {
		t.Fatalf("被拒绝的尝试不应消费重放 ID，want replay-1, got %s", res2.ReplayID)
	}
	if b.Pending() > cfg.MaxPending {
		t.Fatalf("重放越过容量上限: Pending=%d", b.Pending())
	}

	// 组内顺序与死信顺序一致；第一条再次遇不可重试错误，原因与首次可区分。
	first := recvCtx(t, b)
	if first.ID != dead[0] {
		t.Fatalf("group order broken: want %s got %s", dead[0], first.ID)
	}
	if err := b.Nack(first.ID, false, "broken again"); err != nil {
		t.Fatal(err)
	}
	g, _ := b.Get(first.ID)
	if g.Status != StatusDead || g.DeadReason != DeadReplayNonRetryable {
		t.Fatalf("重放后再次死亡原因错误: status=%s reason=%s", g.Status, g.DeadReason)
	}
	if len(g.DeadHistory) != 2 ||
		g.DeadHistory[0].Reason != DeadNonRetryable ||
		g.DeadHistory[1].Reason != DeadReplayNonRetryable {
		t.Fatalf("首次/重放后死亡原因不可区分: %+v", g.DeadHistory)
	}
	t.Logf("判定依据: 释放后整批成功（%s），再次死亡原因 %s 与首次 %s 可区分",
		res2.ReplayID, g.DeadHistory[1].Reason, g.DeadHistory[0].Reason)

	// 其余两条按序投递各一次并确认。
	delivered := []string{first.ID}
	for i := 1; i < len(dead); i++ {
		m := recvCtx(t, b)
		delivered = append(delivered, m.ID)
		if m.ID != dead[i] {
			t.Fatalf("组内顺序错乱 at %d: want %s got %s", i, dead[i], m.ID)
		}
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if delivered[0] != dead[0] {
		t.Fatalf("delivery order: %v", delivered)
	}
}

// 容量（阻塞策略）：整批放不下时调用阻塞；部分释放仍不够时继续等待；
// 容量一旦能容纳整批，一次性整批完成，返回计数与实际状态变化一致。
func TestReplayBlockWaitsAndCompletesWholeBatch(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 4
	cfg.Policy = PolicyBlock
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()

	var dead []string
	for i := 0; i < 3; i++ {
		id, _ := b.Produce(ctx, "gX", []byte("d"))
		kill(t, b, id)
		dead = append(dead, id)
	}
	holders := []string{
		inflight(t, b, "gZ1"),
		inflight(t, b, "gZ2"),
		inflight(t, b, "gZ3"),
	} // 占 3/4，剩 1 < 3：整批放不下
	if b.Pending() != 3 {
		t.Fatalf("setup: Pending=%d, want 3", b.Pending())
	}

	type outcome struct {
		res *ReplayResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		r, e := b.ReplayMessages(ctx, dead)
		done <- outcome{r, e}
	}()
	select {
	case o := <-done:
		t.Fatalf("容量不足时重放应阻塞，却提前返回: %+v", o)
	case <-time.After(150 * time.Millisecond):
	}
	// 释放一个名额：room=2 仍放不下整批 3 条，必须继续阻塞。
	if err := b.Ack(holders[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-done:
		t.Fatalf("部分容量不能触发部分重放: %+v", o)
	case <-time.After(150 * time.Millisecond):
	}
	if m := drainReceive(b, 100*time.Millisecond); m != nil {
		t.Fatalf("阻塞期间不得有重放消息提前进入链路: %s", m.ID)
	}
	// 再释放一个名额：room=3 恰好容纳整批，一次性整批完成。
	if err := b.Ack(holders[1]); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatal(o.err)
		}
		if o.res.Total != 3 || o.res.Replayed != 3 || o.res.Skipped != 0 || o.res.Remaining != 0 {
			t.Fatalf("unexpected replay result: %+v", o.res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("容量释放后重放未完成")
	}
	if b.Pending() != 4 { // holders[2] 仍 inflight + 3 条重放
		t.Fatalf("占用错误: Pending=%d, want 4", b.Pending())
	}
	// 仍在占用名额的 holder 随后确认。
	if err := b.Ack(holders[2]); err != nil {
		t.Fatal(err)
	}
	t.Log("判定依据: 阻塞策略等到容量能容纳整批后一次性完成，计数 Total=3/Replayed=3/Remaining=0")

	// 整批按序各投递一次。
	for i, want := range dead {
		m := recvCtx(t, b)
		if m.ID != want {
			t.Fatalf("顺序错乱 at %d: want %s got %s", i, want, m.ID)
		}
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
}

// 容量（阻塞策略）：等待期间 ctx 取消，什么都不发生；之后容量释放再重放，
// 结果与第一次就成功一致。
func TestReplayBlockCancelChangesNothing(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 2
	cfg.Policy = PolicyBlock
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "gX", []byte("d"))
	kill(t, b, id)
	h1 := inflight(t, b, "gZ1")
	h2 := inflight(t, b, "gZ2") // 占满 2/2，整批 1 条也放不下

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := b.ReplayMessages(ctx, []string{id})
		done <- e
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("取消后阻塞调用未返回")
	}
	m, _ := b.Get(id)
	if m.Status != StatusDead || len(m.ReplayHistory) != 0 {
		t.Fatalf("取消后死信被改动: status=%s history=%d", m.Status, len(m.ReplayHistory))
	}
	if b.Pending() != 2 {
		t.Fatalf("取消后容量占用变化: Pending=%d", b.Pending())
	}
	t.Log("判定依据: ctx 取消后整批未生效，死信仍在死信列表，容量占用不变")

	b.Ack(h1)
	b.Ack(h2)
	res, err := b.ReplayMessages(context.Background(), []string{id})
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Remaining != 0 || res.ReplayID != "replay-1" {
		t.Fatalf("取消后再重放结果异常: %+v", res)
	}
	got := recvCtx(t, b)
	if got.ID != id {
		t.Fatalf("want %s, got %s", id, got.ID)
	}
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}
}

// 原子落盘：整批重放成功后未及消费即"崩溃"，重启后整批都在，每条恰好
// 重放一次；不存在半批状态，也不存在未完成意图需要续跑。
func TestReplayAtomicSnapshotAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxPending = 4
	cfg.Policy = PolicyReject
	var dead []string
	{
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			id, _ := b.Produce(context.Background(), "gR", []byte("d"))
			kill(t, b, id)
			dead = append(dead, id)
		}
		res, err := b.ReplayMessages(context.Background(), dead)
		if err != nil {
			t.Fatal(err)
		}
		if res.Replayed != 3 || res.Remaining != 0 {
			t.Fatalf("unexpected: %+v", res)
		}
		t.Logf("输入: 整批 3 条重放成功后不消费直接 Close（%s 已原子落盘）", res.ReplayID)
		b.Close()
	}

	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	delivered := map[string]int{}
	for i := 0; i < len(dead); i++ {
		m := recvCtx(t, b2)
		delivered[m.ID]++
		if err := b2.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if m := drainReceive(b2, 100*time.Millisecond); m != nil {
		t.Fatalf("重启后出现多余投递: %s", m.ID)
	}
	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("重启后 %s 投递 %d 次（要求恰好一次）", id, delivered[id])
		}
		g, _ := b2.Get(id)
		if g.Round != 1 || len(g.ReplayHistory) != 1 {
			t.Fatalf("重放轨迹错误: round=%d history=%d", g.Round, len(g.ReplayHistory))
		}
		if g.ReplayHistory[0].ReplayID != "replay-1" {
			t.Fatalf("replay id lost across restart: %s", g.ReplayHistory[0].ReplayID)
		}
	}
	t.Logf("判定依据: 重启后整批都在且每条恰好投递一次，计数=%v，无半批/无续跑意图", delivered)
}

// 容量边界上多消费者并发重放同一条死信：恰好一个真正重放，其余幂等跳过，
// 全程不超过容量上限，消息只投递一次，不产生重复在途。
func TestReplayConcurrentAtCapacityReplaysExactlyOnce(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 1 // 死信不占名额；唯一名额先被占用，整批（1 条）放不下
	cfg.Policy = PolicyBlock
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g1", []byte("x"))
	kill(t, b, id)
	holder := inflight(t, b, "g2") // 占满唯一名额

	const n = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	var mu sync.Mutex
	okN := 0
	errKinds := map[string]int{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, rerr := b.ReplayMessages(context.Background(), []string{id})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case rerr == nil:
				okN++
				if res.Replayed != 1 || res.Remaining != 0 {
					t.Errorf("unexpected result: %+v", res)
				}
			case errors.Is(rerr, ErrEmptyReplay):
				errKinds["empty"]++
			case errors.Is(rerr, ErrClosed):
				errKinds["closed"]++
			default:
				t.Errorf("concurrent replay unexpected err: %v", rerr)
			}
		}()
	}

	// 容量采样：任何时刻占用都不得超过 MaxPending。
	maxPending := 0
	stopSample := make(chan struct{})
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				if p := b.Pending(); p > maxPending {
					mu.Lock()
					maxPending = p
					mu.Unlock()
				}
			case <-stopSample:
				return
			}
		}
	}()

	close(start)
	time.Sleep(150 * time.Millisecond) // 8 个调用全部堵在容量上
	if err := b.Ack(holder); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := okN
		mu.Unlock()
		if done == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	if okN != 1 {
		mu.Unlock()
		t.Fatalf("want exactly 1 successful replay, got %d", okN)
	}
	mu.Unlock()

	// 唯一被重放的消息恰好投递一次。
	m := drainReceive(b, time.Second)
	if m == nil || m.ID != id {
		t.Fatalf("want single delivery of %s, got %v", id, m)
	}
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}

	// 其余仍在等待容量的调用：关闭后统一唤醒，它们只能得到幂等跳过/关闭错误。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(stopSample)
	<-sampleDone
	if okN != 1 || errKinds["empty"]+errKinds["closed"] != n-1 {
		t.Fatalf("concurrent tally wrong: ok=%d kinds=%v", okN, errKinds)
	}
	mu.Lock()
	gotMax := maxPending
	mu.Unlock()
	if gotMax > cfg.MaxPending {
		t.Fatalf("并发重放越过容量上限: max Pending=%d", gotMax)
	}
	t.Logf("判定依据: 容量边界 %d 并发重放恰好 1 个生效（empty=%d closed=%d），最大占用=%d，消息仅投递一次",
		okN, errKinds["empty"], errKinds["closed"], gotMax)
}

// 重放后重新获得完整重试机会；再次耗尽回死信，原因可与首次失败区分；
// 再次重放后可正常确认。
func TestReplayRetriesThenReExhaustedDistinguished(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxRetries = 2
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g", []byte("job"))
	// 首次：重试耗尽进死信。
	for {
		m := recvCtx(t, b)
		if err := b.Nack(m.ID, true, "flaky"); err != nil {
			t.Fatal(err)
		}
		g, _ := b.Get(id)
		if g.Status == StatusDead {
			if g.DeadReason != DeadRetryExhausted {
				t.Fatalf("first death reason=%s", g.DeadReason)
			}
			break
		}
	}
	// 重放后继续重试并再次耗尽。
	if _, err := b.ReplayMessages(context.Background(), []string{id}); err != nil {
		t.Fatal(err)
	}
	for {
		m := recvCtx(t, b)
		if m.ID != id || m.Attempts == 0 && m.Round != 1 {
			t.Fatalf("unexpected delivery: %+v", m)
		}
		if err := b.Nack(id, true, "still flaky"); err != nil {
			t.Fatal(err)
		}
		g, _ := b.Get(id)
		if g.Status == StatusDead {
			if g.DeadReason != DeadReplayRetryExhausted {
				t.Fatalf("want %s, got %s", DeadReplayRetryExhausted, g.DeadReason)
			}
			break
		}
	}
	g, _ := b.Get(id)
	if len(g.DeadHistory) != 2 ||
		g.DeadHistory[0].Reason != DeadRetryExhausted ||
		g.DeadHistory[1].Reason != DeadReplayRetryExhausted {
		t.Fatalf("dead history not distinguishable: %+v", g.DeadHistory)
	}
	t.Logf("判定依据: 两次死亡原因可区分: %s -> %s（round=%d）",
		g.DeadHistory[0].Reason, g.DeadHistory[1].Reason, g.Round)

	// 再重放一轮，根因修复后正常确认。
	if _, err := b.ReplayMessages(context.Background(), []string{id}); err != nil {
		t.Fatal(err)
	}
	m := recvCtx(t, b)
	if m.Round != 2 {
		t.Fatalf("want round 2, got %d", m.Round)
	}
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}
	done, _ := b.Get(id)
	if done.Status != StatusAcked || len(done.ReplayHistory) != 2 {
		t.Fatalf("final state wrong: status=%s replays=%d",
			done.Status, len(done.ReplayHistory))
	}
	t.Log("判定依据: 第二次重放 round=2 投递并确认，轨迹完整可查")
}
