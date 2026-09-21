package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

// ErrDuplicate 表示消息已被处理过，本次为重复投递，跳过副作用。
var ErrDuplicate = errors.New("duplicate delivery suppressed")

// Dedup 是消费端幂等器：持久化记录已处理的消息 ID，
// 至少一次投递语义下的重复投递不会产生重复副作用（如重复计费）。
// 处理记录先落盘再执行副作用的提交点由调用方把握：
// 标准用法是 ProcessOnce 内执行副作用，副作用成功后记录落盘；
// 若崩溃发生在副作用之后、落盘之前，重启后该消息会被重放一次，
// 因此副作用本身也应可安全重入。
type Dedup struct {
	mu   sync.Mutex
	set  map[string]bool
	path string
}

type dedupState struct {
	Processed map[string]bool `json:"processed"`
}

// NewDedup 打开（或创建）一个持久化幂等器。
func NewDedup(dir string) (*Dedup, error) {
	d := &Dedup{set: map[string]bool{}, path: filepath.Join(dir, "dedup.json")}
	st := &dedupState{Processed: map[string]bool{}}
	if err := loadJSON(d.path, st); err != nil {
		return nil, err
	}
	d.set = st.Processed
	if d.set == nil {
		d.set = map[string]bool{}
	}
	return d, nil
}

// ProcessOnce 若 id 未处理过则执行 fn 并持久化记录；已处理过则
// 返回 ErrDuplicate，fn 不会执行，杜绝重复副作用。
func (d *Dedup) ProcessOnce(id string, fn func() error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.set[id] {
		return ErrDuplicate
	}
	if err := fn(); err != nil {
		return err // 副作用未成功，不记录，允许重试
	}
	d.set[id] = true
	if err := saveJSON(d.path, &dedupState{Processed: d.set}); err != nil {
		delete(d.set, id)
		return fmt.Errorf("persist dedup: %w", err)
	}
	return nil
}

// Processed 报告 id 是否已处理。
func (d *Dedup) Processed(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.set[id]
}
