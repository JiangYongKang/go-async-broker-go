package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func testConfig(dir string) Config {
	return Config{
		Dir:         dir,
		MaxPending:  8,
		Policy:      PolicyReject,
		MaxRetries:  2,
		BackoffBase: 20 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
	}
}

func recvCtx(t *testing.T, b *Broker) *Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m, err := b.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return m
}

// 基本流程：生产 -> 投递 -> 确认，状态流转可见。
func TestProduceDeliverAck(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, err := b.Produce(context.Background(), "g1", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: produce group=g1 payload=hello -> id=%s", id)

	m := recvCtx(t, b)
	t.Logf("判定依据: 收到 id=%s status=%s attempts=%d", m.ID, m.Status, m.Attempts)
	if m.ID != id || m.Attempts != 1 {
		t.Fatalf("unexpected delivery: %+v", m)
	}
	if err := b.Ack(id); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	got, _ := b.Get(id)
	t.Logf("判定依据: ack 后 status=%s", got.Status)
	if got.Status != StatusAcked {
		t.Fatalf("want acked, got %s", got.Status)
	}
}

// 确认语义：未知消息、重复确认、非 inflight 确认，三类原因可区分。
func TestAckErrorClassification(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	t.Log("输入: ack 未知消息 nope")
	if err := b.Ack("nope"); !errors.Is(err, ErrUnknownMessage) {
		t.Fatalf("want ErrUnknownMessage, got %v", err)
	}
	t.Log("判定依据: ErrUnknownMessage（消息不存在）")

	id, _ := b.Produce(context.Background(), "g", []byte("x"))
	t.Logf("输入: ack 未投递消息 %s", id)
	if err := b.Ack(id); !errors.Is(err, ErrNotInflight) {
		t.Fatalf("want ErrNotInflight, got %v", err)
	}
	t.Log("判定依据: ErrNotInflight（消息仍 pending）")

	recvCtx(t, b)
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 重复 ack 已确认消息 %s", id)
	if err := b.Ack(id); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("want ErrAlreadyAcked, got %v", err)
	}
	t.Log("判定依据: ErrAlreadyAcked（重复确认被拒绝）")

	// 失败不得污染已有状态：消息仍为 acked。
	got, _ := b.Get(id)
	if got.Status != StatusAcked {
		t.Fatalf("state polluted: %s", got.Status)
	}
}

// 分组顺序：同组消息严格按序投递；前一条未确认时后一条不得投递。
func TestGroupOrdering(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var ids []string
	for i := 0; i < 3; i++ {
		id, err := b.Produce(context.Background(), "g1", []byte(fmt.Sprintf("m%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// 另一组一条，验证组间不互相阻塞。
	other, _ := b.Produce(context.Background(), "g2", []byte("other"))
	t.Logf("输入: g1 三条 %v, g2 一条 %s", ids, other)

	first := recvCtx(t, b)
	second := recvCtx(t, b)
	t.Logf("判定依据: 前两次投递 id=%s,%s（g1 与 g2 各一，组内仅一条在途）", first.ID, second.ID)
	if first.ID == second.ID {
		t.Fatal("duplicate delivery")
	}
	// g1 的第二条在第一条确认前不得投递。
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); err == nil {
		t.Fatal("should block: g1 has inflight message")
	}
	t.Log("判定依据: g1 有 inflight，同组下一条被阻塞（顺序保护）")

	// 确认后按序投递剩余消息。
	if err := b.Ack(first.ID); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{ids[1]: true, ids[2]: true, other: true}
	delete(want, second.ID)
	var got []string
	for i := 0; i < 2; i++ {
		m := recvCtx(t, b)
		got = append(got, m.ID)
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("判定依据: 后续投递顺序 %v", got)
	for _, id := range got {
		if !want[id] {
			t.Fatalf("unexpected delivery %s, want one of %v", id, want)
		}
	}
}

// 重试退避与耗尽：可重试错误按退避重投，超过上限进死信，判定依据可区分。
func TestRetryExhaustionToDeadLetter(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxRetries = 2
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g", []byte("job"))
	t.Logf("输入: id=%s MaxRetries=%d 持续 nack(retryable)", id, cfg.MaxRetries)

	for attempt := 1; ; attempt++ {
		m := recvCtx(t, b)
		if m.ID != id {
			t.Fatalf("out of order redelivery: got %s", m.ID)
		}
		t.Logf("第 %d 次投递 attempts=%d -> nack(retryable, cause=flaky)", attempt, m.Attempts)
		if err := b.Nack(id, true, "flaky downstream"); err != nil {
			t.Fatal(err)
		}
		got, _ := b.Get(id)
		t.Logf("判定依据: status=%s error_class=%s dead_reason=%s", got.Status, got.ErrorClass, got.DeadReason)
		if got.Status == StatusDead {
			if got.DeadReason != DeadRetryExhausted {
				t.Fatalf("want reason %s, got %s", DeadRetryExhausted, got.DeadReason)
			}
			break
		}
		if attempt > cfg.MaxRetries+2 {
			t.Fatal("retry did not exhaust")
		}
	}
	dead := b.DeadLetters()
	if len(dead) != 1 || dead[0].ID != id {
		t.Fatalf("dead letters: %+v", dead)
	}
	t.Logf("判定依据: 死信列表含 %s，原因 retry_exhausted（未被静默丢弃）", id)
}

// 不可重试错误：直接进死信，不消耗重试次数。
func TestNonRetryableGoesStraightToDead(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g", []byte("bad"))
	recvCtx(t, b)
	t.Logf("输入: nack(%s, retryable=false, cause=schema invalid)", id)
	if err := b.Nack(id, false, "schema invalid"); err != nil {
		t.Fatal(err)
	}
	got, _ := b.Get(id)
	t.Logf("判定依据: status=%s error_class=%s dead_reason=%s attempts=%d",
		got.Status, got.ErrorClass, got.DeadReason, got.Attempts)
	if got.Status != StatusDead || got.DeadReason != DeadNonRetryable {
		t.Fatalf("want dead/non_retryable, got %s/%s", got.Status, got.DeadReason)
	}
	if got.Attempts != 1 {
		t.Fatalf("non-retryable must not retry, attempts=%d", got.Attempts)
	}
}

// 背压-拒绝策略：达到上限返回 ErrBrokerFull，已接收消息不丢。
func TestBackpressureReject(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 2
	cfg.Policy = PolicyReject
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id1, _ := b.Produce(context.Background(), "g", []byte("a"))
	id2, _ := b.Produce(context.Background(), "g", []byte("b"))
	t.Logf("输入: MaxPending=2，已生产 %s,%s，再生产第三条", id1, id2)
	_, err = b.Produce(context.Background(), "g", []byte("c"))
	if !errors.Is(err, ErrBrokerFull) {
		t.Fatalf("want ErrBrokerFull, got %v", err)
	}
	t.Logf("判定依据: Pending=%d 达到上限 -> ErrBrokerFull", b.Pending())

	// 确认一条后容量释放，可再生产；已有消息不丢。
	recvCtx(t, b)
	if err := b.Ack(id1); err != nil {
		t.Fatal(err)
	}
	id3, err := b.Produce(context.Background(), "g", []byte("c"))
	if err != nil {
		t.Fatalf("after ack, produce: %v", err)
	}
	t.Logf("判定依据: ack 释放容量后生产成功 id=%s，Pending=%d", id3, b.Pending())
	for _, id := range []string{id2, id3} {
		if _, err := b.Get(id); err != nil {
			t.Fatalf("accepted message lost: %s", id)
		}
	}
}

// 背压-阻塞策略：生产者在容量释放前阻塞，之后成功，不丢消息。
func TestBackpressureBlock(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 1
	cfg.Policy = PolicyBlock
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id1, _ := b.Produce(context.Background(), "g", []byte("a"))
	t.Logf("输入: MaxPending=1 Policy=Block，已占 %s，再生产一条（应阻塞）", id1)
	done := make(chan string, 1)
	go func() {
		id, err := b.Produce(context.Background(), "g", []byte("b"))
		if err != nil {
			t.Errorf("blocked produce: %v", err)
		}
		done <- id
	}()
	select {
	case <-done:
		t.Fatal("produce should block at capacity")
	case <-time.After(150 * time.Millisecond):
		t.Log("判定依据: 容量满时生产者阻塞（未返回、未丢消息）")
	}
	m := recvCtx(t, b)
	if err := b.Ack(m.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-done:
		t.Logf("判定依据: 容量释放后阻塞的生产完成 id=%s", id)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked produce did not finish after capacity freed")
	}
}

// 崩溃恢复：inflight 消息重启后回到 pending 重新投递（至少一次），
// 已确认消息不重现，位点不丢。
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	acked, _ := b.Produce(context.Background(), "g", []byte("done"))
	inflight, _ := b.Produce(context.Background(), "g", []byte("wip"))
	m := recvCtx(t, b)
	if m.ID != acked {
		t.Fatalf("want %s, got %s", acked, m.ID)
	}
	if err := b.Ack(acked); err != nil {
		t.Fatal(err)
	}
	m = recvCtx(t, b) // inflight，未确认，模拟崩溃
	if m.ID != inflight {
		t.Fatalf("want %s, got %s", inflight, m.ID)
	}
	if err := b.CommitOffset("cg", 5); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: acked=%s inflight=%s(未确认) offset=5，直接关闭模拟崩溃", acked, inflight)
	b.Close()

	// 重启恢复。
	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	got, _ := b2.Get(inflight)
	t.Logf("判定依据: 恢复后 inflight 消息 status=%s（应回 pending）", got.Status)
	if got.Status != StatusPending {
		t.Fatalf("want pending after recovery, got %s", got.Status)
	}
	m = recvCtx(t, b2)
	if m.ID != inflight {
		t.Fatalf("redelivery mismatch: %s", m.ID)
	}
	t.Logf("判定依据: 未确认消息被重新投递 id=%s（至少一次）", m.ID)
	if off := b2.Offset("cg"); off != 5 {
		t.Fatalf("offset lost: %d", off)
	}
	if _, err := b2.Get(acked); err != nil {
		t.Fatal("acked message lost")
	}
	if g, _ := b2.Get(acked); g.Status != StatusAcked {
		t.Fatalf("acked message resurrected: %s", g.Status)
	}
	t.Log("判定依据: 已确认消息保持 acked 不重现，位点保持 5")
}

// 位点：只允许严格单调前进，回退与重复提交都被拒绝。
func TestOffsetRegressionRejected(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if err := b.CommitOffset("cg", 10); err != nil {
		t.Fatal(err)
	}
	t.Log("输入: committed=10，再提交 7（回退）与 10（重复）")
	for _, off := range []uint64{7, 10} {
		err := b.CommitOffset("cg", off)
		if !errors.Is(err, ErrOffsetRegression) {
			t.Fatalf("offset %d: want ErrOffsetRegression, got %v", off, err)
		}
		t.Logf("判定依据: 提交 %d 被拒 -> ErrOffsetRegression", off)
	}
	if err := b.CommitOffset("cg", 11); err != nil {
		t.Fatalf("forward commit: %v", err)
	}
	if got := b.Offset("cg"); got != 11 {
		t.Fatalf("offset=%d, want 11（失败提交不得污染位点）", got)
	}
	t.Log("判定依据: 前进到 11 成功，被拒绝的提交未污染位点")
}

// 消费端幂等：同一消息重复投递，副作用只执行一次（不重复计费）。
func TestDedupSuppressesDuplicateSideEffects(t *testing.T) {
	dir := t.TempDir()
	d, err := NewDedup(dir)
	if err != nil {
		t.Fatal(err)
	}

	billed := 0
	bill := func() error { billed++; return nil }
	t.Log("输入: 同一消息 msg-1 被投递两次（至少一次语义）")
	if err := d.ProcessOnce("msg-1", bill); err != nil {
		t.Fatal(err)
	}
	err = d.ProcessOnce("msg-1", bill)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	t.Logf("判定依据: 第二次投递返回 ErrDuplicate，计费次数=%d（应为 1）", billed)
	if billed != 1 {
		t.Fatalf("duplicate side effect: billed=%d", billed)
	}

	// 重启后去重记录仍在。
	d2, err := NewDedup(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !d2.Processed("msg-1") {
		t.Fatal("dedup record lost after restart")
	}
	t.Log("判定依据: 重启后去重记录保持，重复投递仍被抑制")
}
