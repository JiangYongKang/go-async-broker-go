package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// BizOp 是一笔可序列化、可幂等重放的业务写入（本地 KV 模拟业务库）。
type BizOp struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// OutboxMessage 是一条待发消息的描述。ID 由调用方给定（确定性），
// 崩溃重放时按 ID 去重，保证幂等。
type OutboxMessage struct {
	ID      string
	Group   string
	Payload []byte
}

// outboxRecord 是一条提交意图：业务写入 + 待发消息，先落盘再应用。
type outboxRecord struct {
	OpID string          `json:"op_id"`
	Biz  []BizOp         `json:"biz"`
	Msgs []OutboxMessage `json:"msgs"`
	Done bool            `json:"done"`
}

// outboxJournal 是意图日志的持久化形式。
type outboxJournal struct {
	Records map[string]*outboxRecord `json:"records"`
}

// bizState 是业务库（KV）的持久化形式；Applied 记录已应用的 opID，
// 保证业务写入幂等，重放不会重复生效。
type bizState struct {
	KV      map[string][]byte `json:"kv"`
	Applied map[string]bool   `json:"applied"`
}

// Outbox 实现业务写入与待发消息的原子提交（事务性发件箱）：
//
//  1. 意图（业务写入 + 待发消息）先写入日志并落盘；
//  2. 应用业务写入（按 opID 去重，幂等）；
//  3. 消息入队（按消息 ID 去重，幂等）；
//  4. 标记意图完成并落盘。
//
// 进程在任意时刻中断后，Recover 会按相同顺序幂等重放未完成的意图，
// 因此不会出现"业务已提交却丢消息"或"消息可见但业务未提交"。
type Outbox struct {
	b       *Broker
	mu      sync.Mutex
	journal *outboxJournal
	biz     *bizState
	jPath   string
	bPath   string
}

// NewOutbox 基于已有 Broker 创建发件箱，并加载日志与业务状态。
func NewOutbox(b *Broker) (*Outbox, error) {
	o := &Outbox{
		b:       b,
		jPath:   filepath.Join(b.cfg.Dir, "outbox.json"),
		bPath:   filepath.Join(b.cfg.Dir, "biz.json"),
		journal: &outboxJournal{Records: map[string]*outboxRecord{}},
		biz: &bizState{
			KV:      map[string][]byte{},
			Applied: map[string]bool{},
		},
	}
	if err := loadJSON(o.jPath, o.journal); err != nil {
		return nil, err
	}
	if err := loadJSON(o.bPath, o.biz); err != nil {
		return nil, err
	}
	return o, nil
}

func loadJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("corrupt file %s: %w", path, err)
	}
	return nil
}

func saveJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// AtomicCommit 原子地应用业务写入并使消息对外可见。
// 同一 opID 重复提交是幂等空操作。已被接受的事务消息不受背压
// 上限影响（容量上限只约束新生产），保证已提交事务不丢消息。
func (o *Outbox) AtomicCommit(opID string, biz []BizOp, msgs ...OutboxMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if rec, ok := o.journal.Records[opID]; ok && rec.Done {
		return nil // 幂等：已完成的提交直接成功
	}
	if _, ok := o.journal.Records[opID]; !ok {
		// 第 1 步：意图落盘。
		o.journal.Records[opID] = &outboxRecord{OpID: opID, Biz: biz, Msgs: msgs}
		if err := saveJSON(o.jPath, o.journal); err != nil {
			delete(o.journal.Records, opID)
			return fmt.Errorf("persist intent: %w", err)
		}
	}
	return o.applyLocked(opID)
}

// applyLocked 按序应用一个意图：业务写入 -> 消息入队 -> 标记完成。
// 每一步都幂等，可在崩溃后安全重入。
func (o *Outbox) applyLocked(opID string) error {
	rec := o.journal.Records[opID]
	// 第 2 步：业务写入（按 opID 去重）。
	if !o.biz.Applied[opID] {
		for _, op := range rec.Biz {
			o.biz.KV[op.Key] = append([]byte(nil), op.Value...)
		}
		o.biz.Applied[opID] = true
		if err := saveJSON(o.bPath, o.biz); err != nil {
			delete(o.biz.Applied, opID)
			return fmt.Errorf("persist biz: %w", err)
		}
	}
	// 第 3 步：消息入队（按消息 ID 去重）。
	o.b.mu.Lock()
	for _, m := range rec.Msgs {
		if _, err := o.b.produceLocked(m.ID, m.Group, m.Payload); err != nil {
			o.b.mu.Unlock()
			return fmt.Errorf("enqueue %s: %w", m.ID, err)
		}
	}
	o.b.mu.Unlock()
	o.b.signal()
	// 第 4 步：标记完成。
	rec.Done = true
	if err := saveJSON(o.jPath, o.journal); err != nil {
		rec.Done = false
		return fmt.Errorf("persist done: %w", err)
	}
	return nil
}

// Recover 在启动时重放所有未完成的意图，幂等补齐业务写入与消息。
func (o *Outbox) Recover() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pending []string
	for id, rec := range o.journal.Records {
		if !rec.Done {
			pending = append(pending, id)
		}
	}
	sort.Strings(pending) // 稳定顺序，便于解释与验证
	for _, id := range pending {
		if err := o.applyLocked(id); err != nil {
			return err
		}
	}
	return nil
}

// BizValue 读取业务库中的值（用于验证业务写入是否生效）。
func (o *Outbox) BizValue(key string) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	v, ok := o.biz.KV[key]
	return append([]byte(nil), v...), ok
}
