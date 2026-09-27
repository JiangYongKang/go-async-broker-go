// Package broker 实现一个本地可运行的异步消息投递与任务处理引擎。
// 用本地文件存储模拟消息代理与消费状态，保证生产、投递、重试、
// 死信与重启恢复各环节结果一致、可解释、可验证。
package broker

import (
	"errors"
	"time"
)

// Status 表示消息在代理中的生命周期状态。
type Status string

const (
	StatusPending  Status = "pending"  // 待投递
	StatusInflight Status = "inflight" // 已投递未确认
	StatusAcked    Status = "acked"    // 已确认
	StatusDead     Status = "dead"     // 已进入死信
)

// ErrorClass 区分可重试错误与不可重试错误。
type ErrorClass string

const (
	ErrClassRetryable    ErrorClass = "retryable"     // 可重试：按退避策略重投
	ErrClassNonRetryable ErrorClass = "non_retryable" // 不可重试：直接进死信
)

// DeadReason 记录消息进入死信的判定依据。
type DeadReason string

const (
	DeadRetryExhausted       DeadReason = "retry_exhausted"        // 首次：重试次数耗尽
	DeadNonRetryable         DeadReason = "non_retryable"          // 首次：不可重试错误
	DeadReplayRetryExhausted DeadReason = "replay_retry_exhausted" // 重放后重试再次耗尽
	DeadReplayNonRetryable   DeadReason = "replay_non_retryable"   // 重放后再次遇不可重试错误
)

// DeadEvent 记录一次"进入死信"判定，重放不抹除历史，可本地追溯。
type DeadEvent struct {
	Round      int        `json:"round"`       // 第几轮：0=首次失败，>=1 为第 N 次重放后
	Reason     DeadReason `json:"reason"`      // 判定原因（可区分首次/重放后）
	ErrorClass ErrorClass `json:"error_class"` // 当时的错误分类
	LastError  string     `json:"last_error,omitempty"`
	Attempts   int        `json:"attempts"` // 该轮已投递次数
	At         time.Time  `json:"at"`
}

// ReplayEvent 记录一次死信重放：从死信重新进入正常投递链路。
type ReplayEvent struct {
	Round     int       `json:"round"`      // 重放轮次（从 1 开始）
	ReplayID  string    `json:"replay_id"`  // 所属重放请求
	FromSeq   uint64    `json:"from_seq"`   // 重放前的旧序号
	ToSeq     uint64    `json:"to_seq"`     // 重放后分配的新序号
	PrevState Status    `json:"prev_state"` // 重放前状态（正常应为 dead）
	At        time.Time `json:"at"`
}

// Message 是代理中的一条消息。
type Message struct {
	ID         string     `json:"id"`
	Group      string     `json:"group"` // 分组键：同组消息严格按序投递
	Seq        uint64     `json:"seq"`   // 全局单调序号，决定投递顺序
	Payload    []byte     `json:"payload"`
	Status     Status     `json:"status"`
	Attempts   int        `json:"attempts"`   // 当前一轮已投递次数（重放时清零，重新获得完整重试机会）
	NotBefore  time.Time  `json:"not_before"` // 重试退避：该时间前不可投递
	LastError  string     `json:"last_error,omitempty"`
	ErrorClass ErrorClass `json:"error_class,omitempty"`
	DeadReason DeadReason `json:"dead_reason,omitempty"`
	Round      int        `json:"round"` // 当前生命周期轮次：0=首次，>=1 表示经过 N 次重放
	// DeadHistory / ReplayHistory 是每轮判定与每次重放的完整轨迹，
	// 重放不会删除既有死信记录，可在本地逐条核对"重放前/后状态与原因"。
	DeadHistory    []DeadEvent     `json:"dead_history,omitempty"`
	ReplayHistory  []ReplayEvent   `json:"replay_history,omitempty"`
	ReplayID       string          `json:"replay_id,omitempty"`       // 当前所属的在途重放请求（重放时写入，死亡/确认后可用于归因）
	PendingReplays map[string]bool `json:"pending_replays,omitempty"` // 已被哪些未完成重放请求覆盖（幂等去重）
}

// ReplayResult 是一次重放请求的结果快照。
type ReplayResult struct {
	ReplayID  string // 本次重放请求的确定性 ID
	Group     string // 按分组重放时为该分组；按消息列表重放为空
	Total     int    // 请求冻结时命中的死信数
	Replayed  int    // 本次调用已重新投入投递的数量
	Skipped   int    // 冻结目标中非死信（已重放/已恢复）被幂等跳过的数量
	Remaining int    // 因容量暂时未能重放、已持久化待后台续跑的数量
}

// ReplayInfo 是一个持久化重放请求的可观测快照。
type ReplayInfo struct {
	ID      string
	Group   string
	Targets []string // 冻结的消息 ID（按旧序号排序）
	Applied map[string]bool
	Done    bool
}

// 可区分的错误原因，调用方可用 errors.Is 判定。
var (
	ErrUnknownMessage   = errors.New("unknown message")       // 对未知消息确认/拒绝
	ErrAlreadyAcked     = errors.New("message already acked") // 重复确认
	ErrNotInflight      = errors.New("message not inflight")  // 消息不在可确认状态
	ErrBrokerFull       = errors.New("broker at capacity")    // 背压：拒绝策略
	ErrOffsetRegression = errors.New("offset regression")     // 位点回退被拒绝
	ErrClosed           = errors.New("broker closed")
	ErrEmptyReplay      = errors.New("no dead letter to replay") // 重放目标中没有死信
)

// BackpressurePolicy 定义达到容量上限时的行为。
type BackpressurePolicy int

const (
	PolicyBlock  BackpressurePolicy = iota // 阻塞生产者直到有容量
	PolicyReject                           // 立即返回 ErrBrokerFull
)

// Config 是引擎配置。
type Config struct {
	Dir         string // 本地存储目录
	MaxPending  int    // 待投递+在途消息容量上限（背压）
	Policy      BackpressurePolicy
	MaxRetries  int           // 可重试错误的最大重试次数，超过进死信
	BackoffBase time.Duration // 退避基数：第 n 次重试等待 BackoffBase << (n-1)
	BackoffMax  time.Duration // 退避上限
}
