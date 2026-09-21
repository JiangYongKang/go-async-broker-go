package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// 原子提交：业务写入与消息同时生效；同 opID 重复提交幂等。
func TestOutboxAtomicCommitAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	b, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	o, err := NewOutbox(b)
	if err != nil {
		t.Fatal(err)
	}

	biz := []BizOp{{Key: "acct/1", Value: []byte("100")}}
	msg := OutboxMessage{ID: "tx1-msg", Group: "billing", Payload: []byte("charge 100")}
	t.Log("输入: op=tx1 biz{acct/1=100} + msg tx1-msg")
	if err := o.AtomicCommit("tx1", biz, msg); err != nil {
		t.Fatal(err)
	}
	v, ok := o.BizValue("acct/1")
	if !ok || string(v) != "100" {
		t.Fatalf("biz not applied: %q ok=%v", v, ok)
	}
	m := recvCtx(t, b)
	if m.ID != "tx1-msg" {
		t.Fatalf("want tx1-msg, got %s", m.ID)
	}
	t.Log("判定依据: 业务值与消息同时可见（原子）")

	// 重复提交同 opID：不得重复入队、不得重复应用。
	if err := o.AtomicCommit("tx1", biz, msg); err != nil {
		t.Fatal(err)
	}
	if n := b.Pending(); n != 1 { // tx1-msg 仍 inflight，无新增
		t.Fatalf("duplicate commit enqueued messages: Pending=%d", n)
	}
	t.Logf("判定依据: 重复提交 op=tx1 后 Pending=%d（幂等，无重复消息）", b.Pending())
}

// 崩溃恢复-场景一：意图已落盘，但业务与消息都未应用。
// Recover 必须补齐两者。
func TestOutboxRecoverBeforeAnyApply(t *testing.T) {
	dir := t.TempDir()
	b, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	// 手工构造"崩溃现场"：只有意图日志。
	journal := &outboxJournal{Records: map[string]*outboxRecord{
		"op1": {OpID: "op1",
			Biz:  []BizOp{{Key: "k", Value: []byte("v")}},
			Msgs: []OutboxMessage{{ID: "m1", Group: "g", Payload: []byte("p")}},
			Done: false},
	}}
	if err := saveJSON(filepath.Join(dir, "outbox.json"), journal); err != nil {
		t.Fatal(err)
	}
	t.Log("输入: 崩溃现场=意图 op1 已落盘，biz/消息均未应用")

	o, err := NewOutbox(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Recover(); err != nil {
		t.Fatal(err)
	}
	if v, ok := o.BizValue("k"); !ok || string(v) != "v" {
		t.Fatalf("biz not recovered: %q ok=%v", v, ok)
	}
	m := recvCtx(t, b)
	if m.ID != "m1" {
		t.Fatalf("message not recovered: %s", m.ID)
	}
	t.Log("判定依据: Recover 补齐业务写入与消息，两者一致可见")
}

// 崩溃恢复-场景二：业务已应用，消息未入队（崩溃点在两者之间）。
// Recover 只补消息，不得重复应用业务。
func TestOutboxRecoverAfterBizBeforeEnqueue(t *testing.T) {
	dir := t.TempDir()
	b, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	journal := &outboxJournal{Records: map[string]*outboxRecord{
		"op2": {OpID: "op2",
			Biz:  []BizOp{{Key: "counter", Value: []byte("1")}},
			Msgs: []OutboxMessage{{ID: "m2", Group: "g", Payload: []byte("x")}},
			Done: false},
	}}
	biz := &bizState{
		KV:      map[string][]byte{"counter": []byte("1")},
		Applied: map[string]bool{"op2": true}, // 业务已应用
	}
	if err := saveJSON(filepath.Join(dir, "outbox.json"), journal); err != nil {
		t.Fatal(err)
	}
	if err := saveJSON(filepath.Join(dir, "biz.json"), biz); err != nil {
		t.Fatal(err)
	}
	t.Log("输入: 崩溃现场=op2 业务已应用(Applied=true)，消息 m2 未入队")

	o, err := NewOutbox(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Recover(); err != nil {
		t.Fatal(err)
	}
	m := recvCtx(t, b)
	if m.ID != "m2" {
		t.Fatalf("want m2, got %s", m.ID)
	}
	if v, _ := o.BizValue("counter"); string(v) != "1" {
		t.Fatalf("biz reapplied or lost: %q", v)
	}
	t.Log("判定依据: 只补消息 m2，业务按 opID 去重未重复应用")
}

// 崩溃恢复-场景三：消息已入队但未标记完成。Recover 不得产生重复消息。
func TestOutboxRecoverAfterEnqueueBeforeDone(t *testing.T) {
	dir := t.TempDir()
	b, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	// 先入队一条与意图中同 ID 的消息，模拟"消息已入队但崩溃于标记完成前"。
	if _, err := b.Produce(context.Background(), "g", []byte("y")); err != nil {
		t.Fatal(err)
	}
	// 把该消息改为确定性 ID 的场景：直接构造意图与已存在的消息。
	journal := &outboxJournal{Records: map[string]*outboxRecord{
		"op3": {OpID: "op3",
			Biz:  []BizOp{{Key: "a", Value: []byte("b")}},
			Msgs: []OutboxMessage{{ID: "msg-1", Group: "g", Payload: []byte("y")}},
			Done: false},
	}}
	if err := saveJSON(filepath.Join(dir, "outbox.json"), journal); err != nil {
		t.Fatal(err)
	}
	t.Log("输入: 崩溃现场=消息 msg-1 已在队列，意图 op3 未标记完成")
	b.Close()

	b2, err := New(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	o, err := NewOutbox(b2)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Recover(); err != nil {
		t.Fatal(err)
	}
	if n := b2.Pending(); n != 1 {
		t.Fatalf("duplicate message after recover: Pending=%d", n)
	}
	t.Logf("判定依据: Recover 后 Pending=%d，消息按 ID 去重无重复", b2.Pending())
	if v, ok := o.BizValue("a"); !ok || string(v) != "b" {
		t.Fatalf("biz not applied: ok=%v", ok)
	}
}

// 状态文件损坏：明确报错而不是静默丢状态（失败可解释）。
func TestCorruptStateFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("输入: 损坏的 state.json")
	_, err := New(testConfig(dir))
	if err == nil {
		t.Fatal("want error for corrupt state")
	}
	t.Logf("判定依据: 启动失败并给出原因: %v", err)
}
