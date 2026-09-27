package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 把一条消息投递后以不可重试错误打进死信。
func killMessage(t *testing.T, b *Broker, id string) {
	t.Helper()
	m := recvCtx(t, b)
	if m.ID != id {
		t.Fatalf("want delivery of %s, got %s", id, m.ID)
	}
	if err := b.Nack(id, false, "poison"); err != nil {
		t.Fatalf("nack %s: %v", id, err)
	}
}

// 主链路：死信按批重放 -> 重新投递/确认 -> 历史与重放记录可查，
// 原死信判定不被抹掉。
func TestReplayDeadBasicAndHistory(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id1, _ := b.Produce(context.Background(), "g", []byte("job-1"))
	id2, _ := b.Produce(context.Background(), "g", []byte("job-2"))
	killMessage(t, b, id1)
	killMessage(t, b, id2)
	if n := len(b.DeadLetters()); n != 2 {
		t.Fatalf("dead letters=%d, want 2", n)
	}
	t.Logf("输入: 死信 %s,%s 按批重放", id1, id2)

	res, err := b.ReplayDead(context.Background(), []string{id1, id2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replayed) != 2 || res.ReplayID == "" {
		t.Fatalf("replay result: %+v", res)
	}
	t.Logf("判定依据: replay_id=%s replayed=%v", res.ReplayID, res.Replayed)

	// 重放后：回到 pending、重试预算重置、死信字段清空、代际+1。
	for _, id := range []string{id1, id2} {
		got, _ := b.Get(id)
		if got.Status != StatusPending || got.Attempts != 0 ||
			got.ReplayGen != 1 || got.DeadReason != "" {
			t.Fatalf("%s after replay: %+v", id, got)
		}
	}
	if n := len(b.DeadLetters()); n != 0 {
		t.Fatalf("dead letters after replay=%d, want 0", n)
	}

	// 像正常消息一样重新走投递与确认。
	m := recvCtx(t, b)
	if m.ID != id1 || m.Attempts != 1 {
		t.Fatalf("redelivery: %+v", m)
	}
	if err := b.Ack(id1); err != nil {
		t.Fatal(err)
	}
	m = recvCtx(t, b)
	if m.ID != id2 {
		t.Fatalf("redelivery: %+v", m)
	}
	if err := b.Ack(id2); err != nil {
		t.Fatal(err)
	}

	// 历史：dead(non_retryable) -> replayed，两次判定都可查。
	h, err := b.History(id1)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 2 || h[0].Kind != EventDead || h[0].Reason != DeadNonRetryable ||
		h[1].Kind != EventReplayed || h[1].ReplayID != res.ReplayID || h[1].ReplayGen != 1 {
		t.Fatalf("history: %+v", h)
	}
	t.Logf("判定依据: history=%s(%s) -> %s(%s gen=%d)，原死信记录保留",
		h[0].Kind, h[0].Reason, h[1].Kind, h[1].ReplayID, h[1].ReplayGen)

	// 重放记录持久可查。
	recs := b.Replays()
	if len(recs) != 1 || !recs[0].Done || len(recs[0].Replayed) != 2 {
		t.Fatalf("replay records: %+v", recs)
	}
}

// 按分组重放：只重放该分组的死信，其他分组不受影响。
func TestReplayDeadGroup(t *testing.T) {
	b, err := New(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	d1, _ := b.Produce(context.Background(), "g1", []byte("a"))
	d2, _ := b.Produce(context.Background(), "g1", []byte("b"))
	d3, _ := b.Produce(context.Background(), "g2", []byte("c"))
	killMessage(t, b, d1)
	killMessage(t, b, d2)
	killMessage(t, b, d3)
	t.Logf("输入: g1 死信 %s,%s；g2 死信 %s；按分组重放 g1", d1, d2, d3)

	res, err := b.ReplayDeadGroup(context.Background(), "g1")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replayed) != 2 {
		t.Fatalf("replayed=%v, want 2", res.Replayed)
	}
	dead := b.DeadLetters()
	if len(dead) != 1 || dead[0].ID != d3 {
		t.Fatalf("remaining dead: %+v", dead)
	}
	t.Logf("判定依据: g1 两条已重放，g2 的 %s 仍在死信（不受影响）", d3)
}

// 幂等与并发：重复调用与多消费者并发重放同一条死信，
// 只产生一条在途消息，副作用（计费）恰好一次。
func TestReplayIdempotentUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	b, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	d, err := NewDedup(dir)
	if err != nil {
		t.Fatal(err)
	}

	id, _ := b.Produce(context.Background(), "g", []byte("job"))
	killMessage(t, b, id)

	// 未知目标：跳过并说明原因，不报错、不留记录。
	res, err := b.ReplayDead(context.Background(), []string{"nope"})
	if err != nil || len(res.Skipped) != 1 || res.Skipped[0].Reason != SkipNotFound {
		t.Fatalf("unknown target: res=%+v err=%v", res, err)
	}
	if n := len(b.Replays()); n != 0 {
		t.Fatalf("no record expected for all-skipped call, got %d", n)
	}
	t.Log("判定依据: 未知消息 -> skipped=not_found，不产生重放记录")

	// 第一次重放成功；紧接着重复调用：目标已不在死信，跳过。
	res1, err := b.ReplayDead(context.Background(), []string{id})
	if err != nil || len(res1.Replayed) != 1 {
		t.Fatalf("first replay: %+v err=%v", res1, err)
	}
	res2, err := b.ReplayDead(context.Background(), []string{id})
	if err != nil || len(res2.Replayed) != 0 ||
		len(res2.Skipped) != 1 || res2.Skipped[0].Reason != SkipNotDead {
		t.Fatalf("duplicate replay: %+v err=%v", res2, err)
	}
	if n := b.Pending(); n != 1 {
		t.Fatalf("Pending=%d, want 1（重复重放不得产生第二条在途消息）", n)
	}
	t.Log("判定依据: 重复调用 -> skipped=not_dead，仍只有一条在途消息")

	// 再次打进死信，然后并发重放。
	killMessage(t, b, id)
	const callers = 8
	results := make(chan *ReplayResult, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := b.ReplayDead(context.Background(), []string{id})
			if err != nil {
				t.Errorf("concurrent replay: %v", err)
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	replayed := 0
	for r := range results {
		replayed += len(r.Replayed)
	}
	if replayed != 1 || b.Pending() != 1 {
		t.Fatalf("concurrent replays produced %d in-flight, Pending=%d", replayed, b.Pending())
	}
	t.Logf("判定依据: %d 个并发调用共重放 %d 条（应为 1），Pending=%d", callers, replayed, b.Pending())

	// 消费侧：恰好一次投递生效，计费一次。
	m := recvCtx(t, b)
	billed := 0
	if err := d.ProcessOnce(m.ID, func() error { billed++; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack(m.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); err == nil {
		t.Fatal("duplicate in-flight message after concurrent replays")
	}
	if billed != 1 {
		t.Fatalf("billed=%d, want 1", billed)
	}
	t.Log("判定依据: 确认后无第二条在途消息，计费恰好一次")
}

// 顺序：重放的消息排在组尾，不得越过同组退避等待与待投递的消息。
func TestReplayRespectsGroupOrder(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.BackoffBase = 500 * time.Millisecond
	cfg.BackoffMax = time.Second
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// m3 先进死信；m1 退避等待中；m2 正常待投递（被 m1 挡住）。
	m3, _ := b.Produce(context.Background(), "g", []byte("dead"))
	killMessage(t, b, m3)
	m1, _ := b.Produce(context.Background(), "g", []byte("backing-off"))
	m := recvCtx(t, b)
	if m.ID != m1 {
		t.Fatalf("want %s, got %s", m1, m.ID)
	}
	if err := b.Nack(m1, true, "flaky"); err != nil { // 进入退避
		t.Fatal(err)
	}
	m2, _ := b.Produce(context.Background(), "g", []byte("queued"))

	if _, err := b.ReplayDead(context.Background(), []string{m3}); err != nil {
		t.Fatal(err)
	}
	g3, _ := b.Get(m3)
	g2, _ := b.Get(m2)
	if g3.Seq <= g2.Seq {
		t.Fatalf("replayed seq=%d must be after queued seq=%d", g3.Seq, g2.Seq)
	}
	g1, _ := b.Get(m1)
	t.Logf("输入: m1 退避中(seq=%d)，m2 待投递(seq=%d)，m3 重放(seq=%d)",
		g1.Seq, g2.Seq, g3.Seq)

	// m1 退避未到期：全组都不得投递（重放的 m3 不能插队）。
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := b.Receive(ctx); err == nil {
		t.Fatal("replayed message jumped ahead of backing-off group head")
	}
	t.Log("判定依据: 组头退避期间无任何投递（重放消息未插队）")

	// 退避结束后按 m1 -> m2 -> m3 顺序投递。
	for _, want := range []string{m1, m2, m3} {
		m := recvCtx(t, b)
		if m.ID != want {
			t.Fatalf("order broken: want %s got %s", want, m.ID)
		}
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("判定依据: 投递顺序 %s -> %s -> %s，组内顺序保持", m1, m2, m3)
}

// 容量：拒绝策略下容量不足返回 ErrBrokerFull 且不留记录；
// 阻塞策略下等待容量释放后完成。
func TestReplayCapacityBackpressure(t *testing.T) {
	t.Run("reject", func(t *testing.T) {
		cfg := testConfig(t.TempDir())
		cfg.MaxPending = 2
		cfg.Policy = PolicyReject
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()

		m1, _ := b.Produce(context.Background(), "g", []byte("a"))
		m2, _ := b.Produce(context.Background(), "g", []byte("b"))
		killMessage(t, b, m1) // m1 死信，占用降到 1
		m3, _ := b.Produce(context.Background(), "g", []byte("c"))
		if n := b.Pending(); n != 2 {
			t.Fatalf("Pending=%d, want 2（已满）", n)
		}
		t.Logf("输入: MaxPending=2 已满（%s,%s），重放死信 %s", m2, m3, m1)

		_, err = b.ReplayDead(context.Background(), []string{m1})
		if !errors.Is(err, ErrBrokerFull) {
			t.Fatalf("want ErrBrokerFull, got %v", err)
		}
		if n := len(b.Replays()); n != 0 {
			t.Fatalf("rejected replay left %d records", n)
		}
		if got, _ := b.Get(m1); got.Status != StatusDead {
			t.Fatalf("rejected replay changed status: %s", got.Status)
		}
		t.Log("判定依据: 容量满 -> ErrBrokerFull，无记录、状态不变（不绕过上限）")

		// 释放容量后重放成功，且排在队尾。
		killMessageCheck := recvCtx(t, b)
		if err := b.Ack(killMessageCheck.ID); err != nil {
			t.Fatal(err)
		}
		res, err := b.ReplayDead(context.Background(), []string{m1})
		if err != nil || len(res.Replayed) != 1 {
			t.Fatalf("replay after capacity freed: %+v err=%v", res, err)
		}
		m := recvCtx(t, b) // m3 在前
		if m.ID != m3 {
			t.Fatalf("want %s before replayed %s, got %s", m3, m1, m.ID)
		}
		if err := b.Ack(m3); err != nil {
			t.Fatal(err)
		}
		m = recvCtx(t, b)
		if m.ID != m1 {
			t.Fatalf("want replayed %s, got %s", m1, m.ID)
		}
		if err := b.Ack(m1); err != nil {
			t.Fatal(err)
		}
		t.Logf("判定依据: 容量释放后重放成功，投递顺序 %s -> %s（队尾）", m3, m1)
	})

	// 整批预算：一批死信整体放不下时必须拒绝，不得先重放一部分、
	// 也不得让占用超过 MaxPending。
	t.Run("reject_batch_over_capacity", func(t *testing.T) {
		cfg := testConfig(t.TempDir())
		cfg.MaxPending = 2
		cfg.Policy = PolicyReject
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()

		d1, _ := b.Produce(context.Background(), "g1", []byte("a"))
		d2, _ := b.Produce(context.Background(), "g2", []byte("b"))
		killMessage(t, b, d1)
		killMessage(t, b, d2)
		// 两条死信、一个待投递槽位：整批需要 2，放不下。
		occ, _ := b.Produce(context.Background(), "g3", []byte("occupy"))
		t.Logf("输入: MaxPending=2，占用=%s，整批重放 %s,%s", occ, d1, d2)

		res, err := b.ReplayDead(context.Background(), []string{d1, d2})
		if !errors.Is(err, ErrBrokerFull) {
			t.Fatalf("want ErrBrokerFull, got res=%+v err=%v", res, err)
		}
		if n := len(b.Replays()); n != 0 {
			t.Fatalf("rejected batch left %d records（必须在落盘前拒绝）", n)
		}
		if n := b.Pending(); n != 1 {
			t.Fatalf("Pending=%d, want 1（不得先重放一部分）", n)
		}
		for _, id := range []string{d1, d2} {
			if got, _ := b.Get(id); got.Status != StatusDead {
				t.Fatalf("%s changed: %s", id, got.Status)
			}
		}
		t.Log("判定依据: 整批放不下 -> ErrBrokerFull，无记录、无部分重放、未越过上限")
	})

	t.Run("block", func(t *testing.T) {
		cfg := testConfig(t.TempDir())
		cfg.MaxPending = 1
		cfg.Policy = PolicyBlock
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()

		m1, _ := b.Produce(context.Background(), "g", []byte("a"))
		killMessage(t, b, m1)
		m2, _ := b.Produce(context.Background(), "g", []byte("b")) // 占满容量
		t.Logf("输入: MaxPending=1 被 %s 占满，阻塞策略下重放 %s", m2, m1)

		done := make(chan *ReplayResult, 1)
		go func() {
			res, err := b.ReplayDead(context.Background(), []string{m1})
			if err != nil {
				t.Errorf("blocked replay: %v", err)
			}
			done <- res
		}()
		select {
		case <-done:
			t.Fatal("replay should block at capacity")
		case <-time.After(100 * time.Millisecond):
			t.Log("判定依据: 容量满时重放阻塞（未绕过上限）")
		}
		m := recvCtx(t, b)
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
		select {
		case res := <-done:
			if len(res.Replayed) != 1 {
				t.Fatalf("blocked replay result: %+v", res)
			}
			t.Log("判定依据: 容量释放后重放完成")
		case <-time.After(2 * time.Second):
			t.Fatal("blocked replay did not finish after capacity freed")
		}
	})
}

// 崩溃恢复：重放到一半进程重启，结果确定——已重放的不重复、
// 未重放的不丢失。
func TestReplayCrashRecoveryDeterministic(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	m1, _ := b.Produce(context.Background(), "g1", []byte("a"))
	m2, _ := b.Produce(context.Background(), "g2", []byte("b"))
	killMessage(t, b, m1)
	killMessage(t, b, m2)

	// 先重放 m1，再手工构造"崩溃现场"：同一批 [m1,m2] 的记录
	// 未标记完成（等价于崩溃发生在 m1 已转换、m2 未转换之间）。
	res, err := b.ReplayDead(context.Background(), []string{m1})
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	rec := b.state.Replays[res.ReplayID]
	rec.IDs = []string{m1, m2}
	rec.Done = false
	if err := b.st.save(b.state); err != nil {
		b.mu.Unlock()
		t.Fatal(err)
	}
	b.mu.Unlock()
	b.Close()
	t.Logf("输入: 崩溃现场=记录 %s 未完成，%s 已重放、%s 仍死信", res.ReplayID, m1, m2)

	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	// 恢复后：m1 不重复重放，m2 被补齐，两者都在 pending。
	g1, _ := b2.Get(m1)
	g2, _ := b2.Get(m2)
	if g1.Status != StatusPending || g1.ReplayGen != 1 {
		t.Fatalf("%s re-replayed or lost: %+v", m1, g1)
	}
	if g2.Status != StatusPending || g2.ReplayGen != 1 {
		t.Fatalf("%s not resumed: %+v", m2, g2)
	}
	if n := b2.Pending(); n != 2 {
		t.Fatalf("Pending=%d, want 2", n)
	}
	h1, _ := b2.History(m1)
	replayedEvents := 0
	for _, e := range h1 {
		if e.Kind == EventReplayed {
			replayedEvents++
		}
	}
	if replayedEvents != 1 {
		t.Fatalf("%s has %d replayed events, want 1（不得重复重放）", m1, replayedEvents)
	}
	recs := b2.Replays()
	if len(recs) != 1 || !recs[0].Done || len(recs[0].Replayed) != 2 {
		t.Fatalf("recovered record: %+v", recs)
	}
	t.Log("判定依据: 恢复补齐 m2、m1 未重复，记录标记完成，结果确定")

	// 两条消息都能正常投递确认；再次重启不会重现。
	for _, want := range []string{m1, m2} {
		m := recvCtx(t, b2)
		if m.ID != want {
			t.Fatalf("want %s, got %s", want, m.ID)
		}
		if err := b2.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	b2.Close()
	b3, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b3.Close()
	if n := b3.Pending(); n != 0 {
		t.Fatalf("Pending=%d after second restart（有消息重现）", n)
	}
	t.Log("判定依据: 确认后重启无消息重现，已完成的记录不再触发重放")
}

// 重放后再次耗尽：原因标记为 replay_*，与首次失败可区分；
// 每次重放都重新获得完整重试预算。
func TestReplayExhaustionDistinguished(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxRetries = 1
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "g", []byte("flaky"))
	// 首次生命周期：重试耗尽进死信。
	for i := 0; i < 2; i++ {
		m := recvCtx(t, b)
		if err := b.Nack(m.ID, true, "flaky"); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := b.Get(id)
	if got.Status != StatusDead || got.DeadReason != DeadRetryExhausted || got.ReplayGen != 0 {
		t.Fatalf("first death: %+v", got)
	}
	t.Logf("输入: %s 首次耗尽进死信(%s)，重放后再次耗尽", id, got.DeadReason)

	res, err := b.ReplayDead(context.Background(), []string{id})
	if err != nil {
		t.Fatal(err)
	}
	got, _ = b.Get(id)
	if got.Attempts != 0 {
		t.Fatalf("attempts not reset: %d（重放应重新获得完整重试预算）", got.Attempts)
	}
	// 重放后的生命周期：重新获得重试机会，再次耗尽。
	for i := 0; i < 2; i++ {
		m := recvCtx(t, b)
		if m.ID != id {
			t.Fatalf("want %s, got %s", id, m.ID)
		}
		if err := b.Nack(m.ID, true, "still flaky"); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = b.Get(id)
	if got.Status != StatusDead || got.DeadReason != DeadReplayExhausted || got.ReplayGen != 1 {
		t.Fatalf("second death: %+v", got)
	}
	t.Logf("判定依据: 再次耗尽 -> %s（与首次 %s 可区分）", got.DeadReason, DeadRetryExhausted)

	h, _ := b.History(id)
	if len(h) != 3 {
		t.Fatalf("history len=%d, want 3: %+v", len(h), h)
	}
	if h[0].Reason != DeadRetryExhausted || h[0].ReplayID != "" ||
		h[1].Kind != EventReplayed || h[1].ReplayID != res.ReplayID ||
		h[2].Kind != EventDead || h[2].Reason != DeadReplayExhausted || h[2].ReplayID != res.ReplayID {
		t.Fatalf("history: %+v", h)
	}
	t.Log("判定依据: 历史完整记录 首次耗尽 -> 重放 -> 重放后耗尽，判定原因各自可查")

	// 重放后的不可重试错误同样可区分。
	id2, _ := b.Produce(context.Background(), "g", []byte("bad"))
	killMessage2 := recvCtx(t, b)
	if err := b.Nack(killMessage2.ID, false, "schema"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReplayDead(context.Background(), []string{id2}); err != nil {
		t.Fatal(err)
	}
	m := recvCtx(t, b)
	if m.ID != id2 {
		t.Fatalf("want %s, got %s", id2, m.ID)
	}
	if err := b.Nack(id2, false, "still bad"); err != nil {
		t.Fatal(err)
	}
	got, _ = b.Get(id2)
	if got.DeadReason != DeadReplayNonRetryable {
		t.Fatalf("want %s, got %s", DeadReplayNonRetryable, got.DeadReason)
	}
	t.Logf("判定依据: 重放后不可重试 -> %s（与首次 %s 可区分）",
		got.DeadReason, DeadNonRetryable)
}
