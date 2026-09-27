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

// 容量：拒绝策略下只重放容量内的一批，其余持久化；容量释放后泵自动续跑完。
func TestReplayRespectsCapacityAndPumpResumes(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 2
	cfg.Policy = PolicyReject
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var dead []string
	for i := 0; i < 3; i++ {
		id, _ := b.Produce(context.Background(), "gX", []byte("d"))
		kill(t, b, id)
		dead = append(dead, id)
	}
	// 占用一个容量名额（inflight），剩 1 个名额。
	occ, _ := b.Produce(context.Background(), "gZ", []byte("occupy"))
	holder := recvCtx(t, b)
	if holder.ID != occ {
		t.Fatalf("want %s, got %s", occ, holder.ID)
	}

	res, err := b.ReplayMessages(context.Background(), dead)
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 1 || res.Remaining != 2 {
		t.Fatalf("want 1 replayed / 2 remaining, got %+v", res)
	}
	if len(b.PendingReplays()) != 1 {
		t.Fatalf("want 1 persisted replay request, got %d", len(b.PendingReplays()))
	}
	if b.Pending() > cfg.MaxPending {
		t.Fatalf("replay bypassed capacity: Pending=%d", b.Pending())
	}
	t.Logf("判定依据: 容量=2 占用 1，重放只推进 %d 条，剩余 %d 条持久化 PendingReplays=%d",
		res.Replayed, res.Remaining, len(b.PendingReplays()))

	// 释放容量：ack 占用消息，泵每收到一次容量信号推进一步，
	// 因此边投递边确认，最终三条重放消息各被投递一次。
	if err := b.Ack(occ); err != nil {
		t.Fatal(err)
	}
	delivered := map[string]int{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && len(delivered) < len(dead) {
		m := drainReceive(b, 200*time.Millisecond)
		if m == nil {
			continue
		}
		delivered[m.ID]++
		if err := b.Ack(m.ID); err != nil { // 每次确认释放容量，驱动泵续跑
			t.Fatal(err)
		}
	}
	if len(b.PendingReplays()) != 0 {
		t.Fatalf("pump did not finish: %+v", b.PendingReplays())
	}
	t.Log("判定依据: 容量逐步释放，后台泵分块续跑完成，未完成请求清空")

	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("message %s delivered %d times（要求恰好一次）", id, delivered[id])
		}
	}
}

// 中途重启：重放推进到一半崩溃，重启后自动续跑；不重放两次、不丢失。
func TestReplayCrashMidwayResumesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxPending = 2
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
		occ, _ := b.Produce(context.Background(), "gQ", []byte("occupy"))
		holder := recvCtx(t, b)
		if holder.ID != occ {
			t.Fatalf("want %s, got %s", occ, holder.ID)
		}
		res, err := b.ReplayMessages(context.Background(), dead)
		if err != nil {
			t.Fatal(err)
		}
		if res.Replayed != 1 || res.Remaining != 2 {
			t.Fatalf("partial replay: %+v", res)
		}
		t.Logf("输入: 重放 1/3 时直接 Close 模拟崩溃（已重放=%s 未重放=%v）",
			dead[0], dead[1:])
		b.Close()
	}

	// 重启：占用消息（崩溃时 inflight）回 pending；未完成重放由泵接管。
	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if len(b2.PendingReplays()) != 1 {
		t.Fatalf("pending replay request lost after restart: %d", len(b2.PendingReplays()))
	}
	// 排空所有消息（占用消息 + 3 条重放），每条死信只应被重放投递一次。
	delivered := map[string]int{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m := drainReceive(b2, 100*time.Millisecond)
		if m == nil {
			if len(b2.PendingReplays()) == 0 {
				break
			}
			continue
		}
		delivered[m.ID]++
		if err := b2.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("after restart message %s delivered %d times（要求恰好一次）", id, delivered[id])
		}
		g, _ := b2.Get(id)
		if len(g.ReplayHistory) != 1 || g.Round != 1 {
			t.Fatalf("message %s replayed twice: round=%d history=%d",
				id, g.Round, len(g.ReplayHistory))
		}
	}
	t.Logf("判定依据: 重启后续跑完成，投递计数=%v，每条仅 1 条重放记录", delivered)
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
