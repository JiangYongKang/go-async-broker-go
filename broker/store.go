package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// store 负责把代理状态持久化到本地文件（原子写：tmp + rename + fsync），
// 并在重启时恢复。所有方法调用方需持有互斥锁。
type store struct {
	path string
}

// state 是持久化的完整状态快照。
type state struct {
	Seq       uint64                    `json:"seq"`
	Messages  map[string]*Message       `json:"messages"`
	Offsets   map[string]uint64         `json:"offsets"`              // 消费组 -> 已提交位点
	Replays   map[string]*replayRequest `json:"replays,omitempty"`    // 未完成的死信重放意图
	ReplaySeq uint64                    `json:"replay_seq,omitempty"` // 重放请求的单调序号
}

func newStore(dir string) *store { return &store{path: filepath.Join(dir, "state.json")} }

func newState() *state {
	return &state{
		Messages: map[string]*Message{},
		Offsets:  map[string]uint64{},
		Replays:  map[string]*replayRequest{},
	}
}

// load 读取状态；文件不存在时返回空状态；文件损坏时返回错误而不是
// 静默丢状态，保证失败可解释。
func (s *store) load() (*state, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	st := newState()
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("corrupt state file %s: %w", s.path, err)
	}
	if st.Messages == nil {
		st.Messages = map[string]*Message{}
	}
	if st.Offsets == nil {
		st.Offsets = map[string]uint64{}
	}
	if st.Replays == nil {
		st.Replays = map[string]*replayRequest{}
	}
	return st, nil
}

// save 原子写入状态快照：先写临时文件并 fsync，再 rename，
// 保证进程在任意时刻崩溃都不会留下半写的状态文件。
func (s *store) save(st *state) error {
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open tmp state: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write tmp state: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync tmp state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close tmp state: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}
