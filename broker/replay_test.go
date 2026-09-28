package broker

import (
	"context"
	"errors"
	"fmt"
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

// 容量：拒绝策略下整批放不下就整批失败——返回 ErrBrokerFull，
// 死信状态/序号/顺序/原因全部不变，不留后台续跑；容量释放后整批成功。
func TestReplayRejectWholeBatchWhenFull(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 4 // 死信不计容量；2 个占用时剩 2 名额 < 整批 3，释放后剩 4 名额可容纳
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
	// 死信不占容量（容量只计 pending+inflight）。两个占用者放在不同分组，
	// 可同时保持 inflight；占用 2 个名额后只剩 2 个名额 < 整批 3 条。
	occ, _ := b.Produce(context.Background(), "gZ1", []byte("occupy"))
	holder := recvCtx(t, b)
	if holder.ID != occ {
		t.Fatalf("want %s, got %s", occ, holder.ID)
	}
	occ2, _ := b.Produce(context.Background(), "gZ2", []byte("occupy2"))
	holder2 := recvCtx(t, b)
	if holder2.ID != occ2 {
		t.Fatalf("want %s, got %s", occ2, holder2.ID)
	}

	// 记录重放前死信的完整状态，拒绝后必须逐字段一致。
	before := map[string]*Message{}
	for _, id := range dead {
		g, _ := b.Get(id)
		before[id] = g
	}

	res, err := b.ReplayMessages(context.Background(), dead)
	if !errors.Is(err, ErrBrokerFull) {
		t.Fatalf("want ErrBrokerFull, got res=%+v err=%v", res, err)
	}
	if res != nil {
		t.Fatalf("rejected replay must not return a partial result: %+v", res)
	}
	t.Log("判定依据: 容量=4 占用 2、整批需 3（只剩 2 名额），重放返回 ErrBrokerFull")

	// 整批不生效：状态、序号、轮次、死信原因、历史全部不变。
	for _, id := range dead {
		g, _ := b.Get(id)
		w := before[id]
		if g.Status != StatusDead || g.Seq != w.Seq || g.Round != w.Round ||
			g.Attempts != w.Attempts || g.DeadReason != w.DeadReason ||
			g.LastError != w.LastError || len(g.ReplayHistory) != len(w.ReplayHistory) ||
			len(g.DeadHistory) != len(w.DeadHistory) {
			t.Fatalf("dead letter %s changed after rejected replay:\nbefore=%+v\nafter =%+v", id, w, g)
		}
	}
	if b.Pending() != 2 {
		t.Fatalf("rejected replay changed occupancy: Pending=%d", b.Pending())
	}
	if len(b.DeadLetters()) != 3 {
		t.Fatalf("dead letters changed after rejection: %d", len(b.DeadLetters()))
	}

	// 没有后台续跑：拒绝后无论等多久都不会自己复活。
	time.Sleep(200 * time.Millisecond)
	if drainReceive(b, 150*time.Millisecond) != nil {
		t.Fatal("rejected batch kept running in the background: unexpected delivery")
	}
	for _, id := range dead {
		if g, _ := b.Get(id); g.Status != StatusDead {
			t.Fatalf("message %s revived without replay call", id)
		}
	}
	t.Log("判定依据: 拒绝后等待无任何投递/复活，不存在后台续跑")

	// 释放一个名额的同时再补一个占用（独立分组），净占用不变：
	// 仍是剩 2 名额 < 3，第二次重放必须继续拒绝且无副作用。
	if err := b.Ack(occ); err != nil {
		t.Fatal(err)
	}
	occ3, _ := b.Produce(context.Background(), "gZ3", []byte("occupy3"))
	if m := recvCtx(t, b); m.ID != occ3 {
		t.Fatalf("want %s, got %s", occ3, m.ID)
	}
	if _, err := b.ReplayMessages(context.Background(), dead); !errors.Is(err, ErrBrokerFull) {
		t.Fatalf("want ErrBrokerFull with 2 slots left for 3, got %v", err)
	}
	for _, id := range dead {
		if g, _ := b.Get(id); g.Status != StatusDead {
			t.Fatalf("dead %s changed after second rejection", id)
		}
	}
	// 释放全部占用：剩 4 个名额 >= 3，整批一次性成功、不超限。
	if err := b.Ack(occ2); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack(occ3); err != nil {
		t.Fatal(err)
	}
	res, err = b.ReplayMessages(context.Background(), dead)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 3 || res.Replayed != 3 || res.Skipped != 0 || res.Remaining != 0 {
		t.Fatalf("whole batch must succeed atomically: %+v", res)
	}
	if b.Pending() > cfg.MaxPending {
		t.Fatalf("replay occupancy wrong: Pending=%d, want <= %d", b.Pending(), cfg.MaxPending)
	}
	t.Logf("判定依据: 容量恰好够（顶格）时整批重放成功 total=%d replayed=%d remaining=%d",
		res.Total, res.Replayed, res.Remaining)

	// 三条各被投递恰好一次。
	delivered := map[string]int{}
	for range dead {
		m := recvCtx(t, b)
		delivered[m.ID]++
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("message %s delivered %d times（要求恰好一次）", id, delivered[id])
		}
	}
}

// 拒绝不留下任何持久化意图：拒绝后重启，死信仍原样保留；整批成功后
// 重启，已复活消息按既有恢复规则回 pending 重投，且只重放一次。
func TestReplayAtomicAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxPending = 2 // 2 个 inflight 占满后，2 条死信整批放不下
	cfg.Policy = PolicyReject

	var dead []string
	{
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			id, _ := b.Produce(context.Background(), "gR", []byte("d"))
			kill(t, b, id)
			dead = append(dead, id)
		}
		// 占满全部容量：整批（2 条）放不下。占用者各自独立分组可同时 inflight。
		for i := 0; i < cfg.MaxPending; i++ {
			occ, _ := b.Produce(context.Background(), fmt.Sprintf("gQ%d", i), []byte("occupy"))
			if m := recvCtx(t, b); m.ID != occ {
				t.Fatalf("want %s, got %s", occ, m.ID)
			}
		}
		if _, err := b.ReplayMessages(context.Background(), dead); !errors.Is(err, ErrBrokerFull) {
			t.Fatalf("want ErrBrokerFull, got %v", err)
		}
		t.Log("输入: 容量打满时整批重放被拒绝，随后直接 Close 模拟崩溃")
		b.Close()
	}

	// 重启：没有未完成意图需要续跑，死信原封不动（含序号与原因）。
	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if dl := b2.DeadLetters(); len(dl) != 2 {
		t.Fatalf("rejected batch persisted across restart: dead=%d", len(dl))
	}
	for _, id := range dead {
		g, _ := b2.Get(id)
		if g.Status != StatusDead || g.Round != 0 || len(g.ReplayHistory) != 0 {
			t.Fatalf("dead letter %s altered across restart: %+v", id, g)
		}
	}
	t.Log("判定依据: 重启后拒绝的批次无任何残留意图，死信状态/序号/原因不变")

	// 排空占用消息释放容量（崩溃时 inflight 回 pending），重放整批成功后
	// 立刻再次崩溃：重启不应重放第二次。
	for i := 0; i < cfg.MaxPending; i++ {
		m := recvCtx(t, b2)
		if err := b2.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if b2.Pending() != 0 {
		t.Fatalf("want capacity fully released, Pending=%d", b2.Pending())
	}
	res, err := b2.ReplayMessages(context.Background(), dead)
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 2 {
		t.Fatalf("want 2 replayed, got %+v", res)
	}
	b2.Close()

	b3, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b3.Close()
	delivered := map[string]int{}
	for {
		m := drainReceive(b3, time.Second)
		if m == nil {
			break
		}
		delivered[m.ID]++
		if err := b3.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("after restart message %s delivered %d times（要求恰好一次）", id, delivered[id])
		}
		g, _ := b3.Get(id)
		if len(g.ReplayHistory) != 1 || g.Round != 1 {
			t.Fatalf("message %s replayed twice across restart: round=%d history=%d",
				id, g.Round, len(g.ReplayHistory))
		}
	}
	t.Logf("判定依据: 整批成功后重启仅触发既有的 inflight/pending 重投，重放记录仍各 1 条: %v", delivered)
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

// 阻塞策略：整批放不下时调用阻塞等待；容量释放后整批完成，
// 等待期间 ctx 取消则什么都不发生。任何时刻不超过容量上限。
func TestReplayBlockWaitsForWholeBatch(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 2 // 2 个 inflight 占满后，2 条死信整批放不下
	cfg.Policy = PolicyBlock
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	var dead []string
	for i := 0; i < 2; i++ {
		id, _ := b.Produce(context.Background(), "gX", []byte("d"))
		kill(t, b, id)
		dead = append(dead, id)
	}
	// 死信不占容量；占满全部 2 个容量名额后，整批（2 条）暂时放不下。
	// 占用者各自使用独立分组，可同时保持 inflight 且不影响 gX 死信。
	var occ []string
	for i := 0; i < cfg.MaxPending; i++ {
		id, _ := b.Produce(context.Background(), fmt.Sprintf("gZ%d", i), []byte("occupy"))
		occ = append(occ, id)
		if m := recvCtx(t, b); m.ID != id {
			t.Fatalf("want %s, got %s", id, m.ID)
		}
	}

	type callResult struct {
		res *ReplayResult
		err error
	}
	done := make(chan callResult, 1)
	go func() {
		res, err := b.ReplayMessages(context.Background(), dead)
		done <- callResult{res, err}
	}()

	// 容量未释放：重放必须还在阻塞，死信原样不动。
	select {
	case r := <-done:
		t.Fatalf("blocking replay returned before capacity freed: %+v %v", r.res, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	for _, id := range dead {
		if g, _ := b.Get(id); g.Status != StatusDead {
			t.Fatalf("dead %s changed while replay blocked", id)
		}
	}
	if b.Pending() != cfg.MaxPending {
		t.Fatalf("blocking replay changed occupancy: %d", b.Pending())
	}
	t.Log("判定依据: 容量满时阻塞策略下重放挂起，无部分生效、无超容")

	// 等待中的 ctx 取消：先确认调用已挂起在容量等待上，再取消，
	// 必须立即返回 context.Canceled 且无副作用。
	{
		ctx, cancel := context.WithCancel(context.Background())
		canceled := make(chan error, 1)
		go func() {
			_, e := b.ReplayMessages(ctx, dead)
			canceled <- e
		}()
		time.Sleep(100 * time.Millisecond) // 等待调用进入容量等待
		cancel()
		select {
		case e := <-canceled:
			if e != context.Canceled {
				t.Fatalf("want context.Canceled, got %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled replay did not return")
		}
		for _, id := range dead {
			if g, _ := b.Get(id); g.Status != StatusDead {
				t.Fatalf("dead %s revived after canceled wait", id)
			}
		}
		t.Log("判定依据: 阻塞中取消 ctx 立即返回，死信无变化")
	}

	// 释放一个名额仍不够（整批要 2 个名额）：继续阻塞。
	if err := b.Ack(occ[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		t.Fatalf("replay proceeded with only 1 slot for 2: %+v", r.res)
	case <-time.After(200 * time.Millisecond):
	}
	// 释放第二个名额：整批一次性完成。
	if err := b.Ack(occ[1]); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.res.Total != 2 || r.res.Replayed != 2 || r.res.Skipped != 0 || r.res.Remaining != 0 {
			t.Fatalf("want whole batch done, got %+v", r.res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking replay never completed after capacity freed")
	}
	if b.Pending() > cfg.MaxPending {
		t.Fatalf("capacity exceeded: Pending=%d", b.Pending())
	}
	t.Log("判定依据: 容量足以容纳整批后一次性完成，计数 total=2 replayed=2 remaining=0")

	delivered := map[string]int{}
	for range dead {
		m := recvCtx(t, b)
		delivered[m.ID]++
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range dead {
		if delivered[id] != 1 {
			t.Fatalf("%s delivered %d times", id, delivered[id])
		}
	}
}

// 容量边界下多个消费者并发重放同一条死信：恰好一个真正复活，
// 其余得到确定结果（ErrBrokerFull 或 ErrEmptyReplay），
// 不会出现两条在途消息，也不超过容量上限。
func TestReplayConcurrentAtCapacityExactlyOnce(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.MaxPending = 1 // 1 个 inflight 占满后，1 条死信也放不下
	cfg.Policy = PolicyReject
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	id, _ := b.Produce(context.Background(), "gC", []byte("x"))
	kill(t, b, id)
	// 死信不占容量；占满全部容量，使整批（1 条）此刻放不下。
	var occ []string
	for i := 0; i < cfg.MaxPending; i++ {
		o, _ := b.Produce(context.Background(), "gZ", []byte("occupy"))
		occ = append(occ, o)
		if m := recvCtx(t, b); m.ID != o {
			t.Fatalf("want %s, got %s", o, m.ID)
		}
	}

	// 阶段一：容量满时 4 路并发重放，必须全部得到 ErrBrokerFull，
	// 且没有任何一条把死信复活（整批不生效）。
	const fullCallers = 4
	var wg sync.WaitGroup
	var mu sync.Mutex
	var okN, fullN, emptyN int
	runCallers := func(n int, gate chan struct{}) {
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if gate != nil {
					<-gate
				}
				_, err := b.ReplayMessages(context.Background(), []string{id})
				mu.Lock()
				switch {
				case err == nil:
					okN++
				case errors.Is(err, ErrBrokerFull):
					fullN++
				case errors.Is(err, ErrEmptyReplay):
					emptyN++
				default:
					t.Errorf("unexpected replay error: %v", err)
				}
				mu.Unlock()
			}()
		}
		close(start)
	}
	runCallers(fullCallers, nil)
	wg.Wait()
	if fullN != fullCallers || okN != 0 {
		t.Fatalf("at full capacity: want all %d ErrBrokerFull, got ok=%d full=%d empty=%d",
			fullCallers, okN, fullN, emptyN)
	}
	if g, _ := b.Get(id); g.Status != StatusDead {
		t.Fatal("a rejected concurrent replay revived the message")
	}
	t.Logf("判定依据: 满容时 %d 路并发重放全部 ErrBrokerFull，死信未复活", fullCallers)

	// 阶段二：释放容量的同时放行另外 4 路并发重放，恰好一个成功。
	const freeCallers = 4
	release := make(chan struct{})
	runCallers(freeCallers, release)
	for _, o := range occ {
		if err := b.Ack(o); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	wg.Wait()

	if okN != 1 {
		t.Fatalf("want exactly 1 successful replay, got ok=%d full=%d empty=%d", okN, fullN, emptyN)
	}
	if emptyN != freeCallers-1 {
		t.Fatalf("losing callers after release must get ErrEmptyReplay: ok=%d full=%d empty=%d",
			okN, fullN, emptyN)
	}
	if b.Pending() > cfg.MaxPending {
		t.Fatalf("concurrent replay exceeded capacity: Pending=%d", b.Pending())
	}
	if m := drainReceive(b, time.Second); m == nil || m.ID != id {
		t.Fatalf("want single delivery of %s, got %v", id, m)
	}
	if drainReceive(b, 200*time.Millisecond) != nil {
		t.Fatal("duplicate in-flight delivery from concurrent replay")
	}
	if err := b.Ack(id); err != nil {
		t.Fatal(err)
	}
	t.Logf("判定依据: 释放容量后 %d 路并发重放成功=%d 空重放=%d（满容阶段拒绝=%d），仅一条在途",
		freeCallers, okN, emptyN, fullN)
}
