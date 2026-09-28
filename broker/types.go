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
	DeadHistory   []DeadEvent   `json:"dead_history,omitempty"`
	ReplayHistory []ReplayEvent `json:"replay_history,omitempty"`
	ReplayID      string        `json:"replay_id,omitempty"` // 最近一次复活本条消息的重放请求 ID（死亡/确认后可用于归因）
}

// ReplayResult 是一次重放请求的结果快照。
//
// 整批原子语义下只有两种确定结果：
//   - 放得下：整批重放成功，Replayed+Skipped==Total、Remaining==0；
//   - 放不下（PolicyReject）：返回 ErrBrokerFull，结果为 nil，整批不生效。
type ReplayResult struct {
	ReplayID  string // 本次重放请求的确定性 ID
	Group     string // 按分组重放时为该分组；按消息列表重放为空
	Total     int    // 冻结目标总数（含调用时已不是死信的目标）
	Replayed  int    // 本次调用真正从 dead 置回 pending 的数量
	Skipped   int    // 目标中已不是死信（重复/并发调用被别的重放抢先复活）而幂等跳过的数量
	Remaining int    // 恒为 0：整批语义下不存在"部分留待后台续跑"
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
