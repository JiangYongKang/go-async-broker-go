package broker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 多生产者、多消费者与一次中途崩溃恢复并发运行：
// 每条消息恰好计费一次（幂等），无丢失、无重复副作用、无数据竞态。
func TestConcurrentProduceConsumeWithRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxPending = 64
	cfg.Policy = PolicyBlock

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDedup(dir)
	if err != nil {
		t.Fatal(err)
	}

	const producers = 4
	const perProducer = 25
	const total = producers * perProducer
	groups := []string{"g1", "g2", "g3"}

	var billMu sync.Mutex
	billed := map[string]int{}

	var wgProd sync.WaitGroup
	for p := 0; p < producers; p++ {
		wgProd.Add(1)
		go func(p int) {
			defer wgProd.Done()
			for i := 0; i < perProducer; i++ {
				payload := fmt.Sprintf("p%d-%d", p, i)
				_, err := b.Produce(context.Background(), groups[i%len(groups)], []byte(payload))
				if err != nil {
					t.Errorf("produce: %v", err)
					return
				}
			}
		}(p)
	}

	// 消费到一半时并发执行一次"崩溃恢复"（关闭并重开）。
	var wgCons sync.WaitGroup
	var ackedMu sync.Mutex
	ackedCount := 0
	stop := make(chan struct{})
	consume := func(b *Broker) {
		defer wgCons.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			m, err := b.Receive(ctx)
			cancel()
			if err != nil {
				continue
			}
			// 幂等处理：重复投递不重复计费。
			if err := d.ProcessOnce(m.ID, func() error {
				billMu.Lock()
				billed[m.ID]++
				billMu.Unlock()
				return nil
			}); err != nil && err != ErrDuplicate {
				t.Errorf("process: %v", err)
			}
			if err := b.Ack(m.ID); err != nil {
				t.Errorf("ack %s: %v", m.ID, err)
				continue
			}
			ackedMu.Lock()
			ackedCount++
			ackedMu.Unlock()
		}
	}
	for c := 0; c < 3; c++ {
		wgCons.Add(1)
		go consume(b)
	}

	wgProd.Wait()
	// 等待全部消息确认完成。
	deadline := time.Now().Add(15 * time.Second)
	for {
		ackedMu.Lock()
		n := ackedCount
		ackedMu.Unlock()
		if n >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d acked", n, total)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 模拟崩溃恢复：关闭后重开，状态必须一致。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	wgCons.Wait()
	defer b2.Close()

	t.Logf("输入: %d 生产者 x %d 条，3 消费者，中途一次重启", producers, perProducer)
	t.Logf("判定依据: acked=%d unique_billed=%d Pending=%d", total, len(billed), b2.Pending())
	if len(billed) != total {
		t.Fatalf("billed %d unique, want %d（有丢失或重复）", len(billed), total)
	}
	for id, n := range billed {
		if n != 1 {
			t.Fatalf("message %s billed %d times（重复计费）", id, n)
		}
	}
	if p := b2.Pending(); p != 0 {
		t.Fatalf("Pending=%d after drain（有消息滞留）", p)
	}
}

// 分组顺序在并发重试下仍稳定：同组消息的投递序号单调。
func TestGroupOrderStableUnderConcurrentRetry(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxPending = 32 // 容量需容纳本测试的全部消息
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	const n = 10
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := b.Produce(context.Background(), "ordered", []byte(fmt.Sprintf("%02d", i)))
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, id)
	}
	t.Logf("输入: 同组 %d 条消息，每条首次投递先 nack 再接受", n)

	nacked := map[string]bool{}
	var got []string
	for len(got) < n {
		m := recvCtx(t, b)
		if !nacked[m.ID] {
			nacked[m.ID] = true
			if err := b.Nack(m.ID, true, "flaky"); err != nil {
				t.Fatal(err)
			}
			continue
		}
		got = append(got, m.ID)
		if err := b.Ack(m.ID); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("判定依据: 重试后投递顺序 %v", got)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order broken at %d: want %v got %v", i, want, got)
		}
	}
}
