// demo 演示引擎的完整链路：原子提交 -> 有序投递 -> 失败重试 ->
// 死信 -> 崩溃恢复。运行：go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	broker "github.com/highcumontoa/go-async-broker-go/broker"
)

func main() {
	dir, err := os.MkdirTemp("", "broker-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := broker.Config{
		Dir: dir, MaxPending: 16, Policy: broker.PolicyReject,
		MaxRetries: 2, BackoffBase: 50 * time.Millisecond, BackoffMax: time.Second,
	}
	b, err := broker.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// 1. 事务性发件箱：业务写入与消息原子提交。
	o, err := broker.NewOutbox(b)
	if err != nil {
		log.Fatal(err)
	}
	if err := o.Recover(); err != nil {
		log.Fatal(err)
	}
	err = o.AtomicCommit("tx-1",
		[]broker.BizOp{{Key: "acct/42", Value: []byte("-100")}},
		broker.OutboxMessage{ID: "charge-42", Group: "billing", Payload: []byte("charge 100")})
	if err != nil {
		log.Fatal(err)
	}
	v, _ := o.BizValue("acct/42")
	fmt.Printf("[outbox] 业务已提交 acct/42=%s，消息 charge-42 已入队\n", v)

	// 2. 普通生产：同组三条，验证组内顺序。
	for i := 0; i < 3; i++ {
		if _, err := b.Produce(context.Background(), "jobs", []byte(fmt.Sprintf("job-%d", i))); err != nil {
			log.Fatal(err)
		}
	}

	// 3. 消费：charge-42 先失败两次（重试退避），jobs 依次确认。
	d, err := broker.NewDedup(dir)
	if err != nil {
		log.Fatal(err)
	}
	failed := map[string]int{}
	processed := 0
	for processed < 4 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		m, err := b.Receive(ctx)
		cancel()
		if err != nil {
			log.Fatal(err)
		}
		err = d.ProcessOnce(m.ID, func() error {
			fmt.Printf("[consume] group=%s id=%s attempts=%d payload=%s\n",
				m.Group, m.ID, m.Attempts, m.Payload)
			return nil
		})
		if errors.Is(err, broker.ErrDuplicate) {
			fmt.Printf("[consume] id=%s 重复投递被幂等抑制\n", m.ID)
		}
		if m.ID == "charge-42" && failed[m.ID] < 2 {
			failed[m.ID]++
			if err := b.Nack(m.ID, true, "downstream flaky"); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("[retry] id=%s 第 %d 次失败，退避后重试\n", m.ID, failed[m.ID])
			continue
		}
		if err := b.Ack(m.ID); err != nil {
			log.Fatal(err)
		}
		processed++
	}

	// 4. 不可重试错误直接进死信。
	badID, _ := b.Produce(context.Background(), "jobs", []byte("bad-schema"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	m, _ := b.Receive(ctx)
	cancel()
	if err := b.Nack(m.ID, false, "schema invalid"); err != nil {
		log.Fatal(err)
	}
	for _, dl := range b.DeadLetters() {
		fmt.Printf("[dead] id=%s reason=%s last_error=%s\n", dl.ID, dl.DeadReason, dl.LastError)
	}
	_ = badID

	// 5. 模拟崩溃恢复：未确认消息重启后重投。
	pendingID, _ := b.Produce(context.Background(), "jobs", []byte("will-crash"))
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	crashed, _ := b.Receive(ctx) // 投递但不确认
	cancel()
	b.Close()
	fmt.Printf("[crash] id=%s 投递后未确认，进程退出\n", crashed.ID)

	b2, err := broker.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer b2.Close()
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	redelivered, err := b2.Receive(ctx)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[recover] 重启后重投 id=%s（原 id=%s）\n", redelivered.ID, pendingID)
	if err := b2.Ack(redelivered.ID); err != nil {
		log.Fatal(err)
	}
	fmt.Println("[done] 演示完成：顺序、重试、死信、恢复均符合预期")
}
