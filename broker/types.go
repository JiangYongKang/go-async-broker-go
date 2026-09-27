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
	DeadRetryExhausted     DeadReason = "retry_exhausted"        // 首次生命周期内重试次数耗尽
	DeadNonRetryable       DeadReason = "non_retryable"          // 首次生命周期内不可重试错误
	DeadReplayExhausted    DeadReason = "replay_retry_exhausted" // 重放后再次耗尽重试
	DeadReplayNonRetryable DeadReason = "replay_non_retryable"   // 重放后再次被判定不可重试
)

// Message 是代理中的一条消息。
type Message struct {
	ID         string     `json:"id"`
	Group      string     `json:"group"` // 分组键：同组消息严格按序投递
	Seq        uint64     `json:"seq"`   // 全局单调序号，决定投递顺序
	Payload    []byte     `json:"payload"`
	Status     Status     `json:"status"`
	Attempts   int        `json:"attempts"`   // 已投递次数
	NotBefore  time.Time  `json:"not_before"` // 重试退避：该时间前不可投递
	LastError  string     `json:"last_error,omitempty"`
	ErrorClass ErrorClass `json:"error_class,omitempty"`
	DeadReason DeadReason `json:"dead_reason,omitempty"`
	// ReplayGen 记录消息已被成功重放的次数：0 表示首次生命周期；
	// 再次进入死信时据此把原因标记为 replay_*，与首次失败区分。
	ReplayGen int `json:"replay_gen,omitempty"`
	// OriginReplayID 记录当前生命周期由哪次重放发起（首次生命周期为空），
	// 再次进死信时写入 dead 事件，便于把两次判定关联起来。
	OriginReplayID string `json:"origin_replay_id,omitempty"`
}

// 可区分的错误原因，调用方可用 errors.Is 判定。
var (
	ErrUnknownMessage   = errors.New("unknown message")       // 对未知消息确认/拒绝
	ErrAlreadyAcked     = errors.New("message already acked") // 重复确认
	ErrNotInflight      = errors.New("message not inflight")  // 消息不在可确认状态
	ErrBrokerFull       = errors.New("broker at capacity")    // 背压：拒绝策略
	ErrOffsetRegression = errors.New("offset regression")     // 位点回退被拒绝
	ErrClosed           = errors.New("broker closed")
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
