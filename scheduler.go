// Package tenantsched 模拟单颗 CPU 上的租户层级调度，不启动任何真实作业。
//
// 组构成最多四层、20 个节点的树；每个组拥有按周期重置的运行配额，
// 周期长度固定为 PeriodTicks 个整数 tick。作业属于某个组，调度器每个
// tick 至多执行一个作业一个单位的工作量，并同时沿作业所在组到根的
// 路径扣减所有祖先组的配额。
//
// 所有写接口（提交、迁组、取消、兄弟组配额转让、推进时间）都携带期望
// 修订号，构成乐观并发控制：修订号不匹配则拒绝且不产生任何副作用。修订号
// 在每次成功的已提交变更以及每个已执行 tick 之后单调递增。
package tenantsched

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// PeriodTicks 是配额重置周期的长度：进入 tick t 时若 t%PeriodTicks == 0，
// 所有组的本周期已用配额先清零，再处理本 tick 已提交的变更。
const PeriodTicks = 10

// 结构与规模上限。
const (
	MaxGroups    = 20 // 组树节点数上限
	MaxTreeDepth = 4  // 组树最大深度（根为第 1 层）
	MaxJobs      = 40 // 作业总数上限
	CPUCount     = 1  // 模拟单颗 CPU
	WorkPerTick  = 1  // 每个 tick 至多完成的工作量
)

// 哨兵错误：调用方可以用 errors.Is 判定具体冲突类别。
var (
	// ErrInvalidArgument 表示参数本身非法（未知组、树不合法、零工作量等）。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrLimitExceeded 表示超过结构或规模上限。
	ErrLimitExceeded = errors.New("limit exceeded")
	// ErrConflict 表示期望修订号与当前修订号不一致。
	ErrConflict = errors.New("revision conflict")
	// ErrNotFound 表示引用的组或作业不存在。
	ErrNotFound = errors.New("not found")
	// ErrJobTerminal 表示对已完成或已取消的作业执行迁组/取消等操作。
	ErrJobTerminal = errors.New("job terminal")
)

// JobState 是作业的生命周期状态。
type JobState string

const (
	JobReady     JobState = "READY"     // 已释放、未完成
	JobCompleted JobState = "COMPLETED" // 剩余工作量归零
	JobCanceled  JobState = "CANCELED"  // 被取消
)

// TickKind 描述一个 tick 的调度结果类别。
type TickKind string

const (
	// TickRan 表示本 tick 执行了一个就绪作业一个单位的工作量。
	TickRan TickKind = "RAN"
	// TickIdleNotReady 表示不存在已释放且仍可运行的作业。
	TickIdleNotReady TickKind = "IDLE_NOT_READY"
	// TickIdleBlocked 表示有可运行的就绪作业，但所有候选都被某个
	// 已耗尽配额的祖先组挡住。
	TickIdleBlocked TickKind = "IDLE_BLOCKED"
)

// ChangeKind 标识在某个 tick 开头被处理的已提交变更类型。
type ChangeKind string

const (
	ChangeSubmit   ChangeKind = "SUBMIT"
	ChangeMigrate  ChangeKind = "MIGRATE"
	ChangeCancel   ChangeKind = "CANCEL"
	ChangeTransfer ChangeKind = "TRANSFER"
)

// GroupSpec 描述组树的一个节点。根的 Parent 必须为空字符串。
type GroupSpec struct {
	ID     string
	Parent string
	// Quota 是每个配额周期内该组允许执行的 tick 数；必须为正。
	Quota int
}

// JobSpec 描述一个待提交作业。
type JobSpec struct {
	ID        string
	ReleaseAt uint64 // 释放时刻（整数 tick）：tick t 时 ReleaseAt <= t 才就绪
	Work      int    // 剩余工作量，必须为正
	Deadline  uint64 // 截止 tick：tick Deadline 结束时仍未完成即为超期
	Group     string // 所属组（迁组后会更新）
}

// AppliedChange 记录在某个 tick 开头被处理的一条已提交变更。
// 作业在提交的同一 tick 就参与调度（ReleaseAt 仍需满足）。
type AppliedChange struct {
	Kind     ChangeKind
	JobID    string
	Revision uint64 // 该变更提交成功时的修订号
	// Group：SUBMIT 时为提交组，MIGRATE 时为目标组，CANCEL 时为空。
	Group string

	// 仅 TRANSFER 使用：From 是捐出组、Group 是受让组、Amount 是转让量。
	From   string
	Amount int
}

// TraceEntry 是一个 tick 的完整轨迹。
type TraceEntry struct {
	Tick uint64

	Kind TickKind
	// Ran 为 true 时作业被执行了 WorkPerTick 个单位的工作量。
	Ran    bool
	JobID  string
	Group  string // 被执行作业当时所属组
	Period uint64 // 当前配额周期序号 tick / PeriodTicks

	// Applied 是本 tick 开头处理的全部已提交变更，按提交修订号排序。
	Applied []AppliedChange

	// Path 是被执行作业从所属组到根经过的全部组（含自身与根）。
	Path []string
	// Deducted 与 Path 对齐：每个祖先组本周期扣减后的已用配额。
	Deducted []int

	// 空转（IDLE_BLOCKED）时记录的阻挡祖先。
	// 选择规则见 Scheduler 文档：按（截止 tick，作业 ID）排序的第一个
	// 就绪作业，其到根路径上最深（离该作业最近）的配额耗尽祖先。
	BlockedBy string
	// 空转时仍处于就绪状态的作业数量，便于核对空转原因。
	ReadyCount int
}

// OverdueEvidence 是一次超期判定证据：作业在其截止 tick 结束时仍有
// 剩余工作量。同一作业只记录一次，证据随截止 tick 的轨迹产出
// （DetectedAt == Deadline）。
type OverdueEvidence struct {
	JobID      string
	Deadline   uint64
	DetectedAt uint64 // 发现超期的 tick（等于 Deadline）
	Group      string // 发现时所属组
	Remaining  int    // 发现时剩余工作量
	State      JobState
	// Blocker 是该作业到根路径上最深的配额耗尽祖先；没有则为空。
	Blocker string
}

// QuotaView 是某个组在某个时刻的配额视图。
type QuotaView struct {
	GroupID string
	Period  uint64 // 当前周期序号
	// Quota 是基础额度（每周期固定）；Effective 是本周期有效额度，即
	// 基础额度叠加同周期内同父兄弟组转让净额后的结果。兄弟组之间的
	// 临时转让只改有效额度，绝不改变父组或任何祖先组的额度。
	Quota     int
	Effective int
	Used      int
}

// JobView 是作业的只读快照。
type JobView struct {
	ID        string
	Group     string
	ReleaseAt uint64
	Deadline  uint64
	Work      int // 初始工作量
	Remaining int
	State     JobState
	Overdue   bool
}

// Snapshot 是某一时刻调度器的完整只读状态。
type Snapshot struct {
	Now      uint64
	Revision uint64
	Groups   []GroupSpec
	Jobs     []JobView
	Quotas   []QuotaView
}

// CommitResult 是提交/迁组/取消的返回值。
type CommitResult struct {
	Revision uint64
	Now      uint64
}

// AdvanceResult 是推进时间的返回值。
type AdvanceResult struct {
	FromTick uint64 // 推进前的 now（不含）
	ToTick   uint64 // 推进后的 now（不含；执行的 tick 为 [FromTick, ToTick)）
	Ticks    []TraceEntry
	Overdue  []OverdueEvidence // 仅包含本次推进期间新发现的超期证据
	Revision uint64
}

// groupNode 是组树节点的内部表示。
type groupNode struct {
	spec GroupSpec
	used int // 当前周期已用配额
	// adjust 是本周期兄弟组转让净额（转出为负、转入为正）。有效额度 =
	// spec.Quota + adjust。周期边界与 used 一起归零；本字段只跟踪已在
	// 某个 tick 开头入账、对调度生效的转让，提交后尚待入账的转让在
	// pending 中投影（见 projectedLocked）。
	adjust int
}

// jobState 是作业的内部可变状态。
type jobState struct {
	spec      JobSpec
	group     string
	remaining int
	state     JobState
	overdue   bool
}

// committedChange 是已提交但尚未在某个 tick 开头“处理”的变更。
type committedChange struct {
	kind     ChangeKind
	jobID    string
	group    string
	revision uint64

	// 仅 TRANSFER 使用：from 为捐出组、group 为受让组、amount 为转让量。
	from   string
	amount int
}

// Scheduler 是并发安全的单 CPU 租户调度模拟器。
//
// 单把互斥锁串行化所有公开操作：任何时刻最多有一个 Advance 正在推进
// 任意 tick，因此并发推进不可能重复执行同一个 tick。已提交变更在提交
// 成功的瞬间对状态生效，但会计入“下一个进入的 tick 开头处理的变更”
// 列表，严格满足“每个 tick 先处理已提交变更，再选择作业”的顺序。
type Scheduler struct {
	mu sync.Mutex

	now      uint64
	revision uint64

	groups map[string]*groupNode
	loans  map[string]*LoanReceipt

	jobs map[string]*jobState

	// pending 是自上一个 tick 以来成功提交、尚待记入轨迹的变更，
	// 按提交顺序（即修订号）排列，在下一个 tick 开头进入该 tick 的
	// Applied 列表。
	pending []committedChange

	trace   []TraceEntry
	overdue []OverdueEvidence
}

// New 创建调度器。groups 描述一棵最多 MaxGroups 个节点、最深 MaxTreeDepth
// 层的树；必须恰好包含一个根（Parent 为空），不可成环或悬空。
func New(groups []GroupSpec) (*Scheduler, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("%w: group tree must not be empty", ErrInvalidArgument)
	}
	if len(groups) > MaxGroups {
		return nil, fmt.Errorf("%w: %d groups exceeds max %d", ErrLimitExceeded, len(groups), MaxGroups)
	}

	nodes := make(map[string]*groupNode, len(groups))
	children := make(map[string][]string, len(groups))
	var roots int
	var rootID string

	for _, g := range groups {
		if g.ID == "" {
			return nil, fmt.Errorf("%w: empty group id", ErrInvalidArgument)
		}
		if _, dup := nodes[g.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate group id %q", ErrInvalidArgument, g.ID)
		}
		if g.Quota <= 0 {
			return nil, fmt.Errorf("%w: group %q quota must be positive, got %d", ErrInvalidArgument, g.ID, g.Quota)
		}
		if g.Parent == "" {
			roots++
			rootID = g.ID
		}
		nodes[g.ID] = &groupNode{spec: g}
	}
	if roots != 1 {
		return nil, fmt.Errorf("%w: group tree must have exactly one root, got %d", ErrInvalidArgument, roots)
	}

	// 挂上父子边并校验父节点存在。
	for _, g := range groups {
		if g.Parent == "" {
			continue
		}
		if _, ok := nodes[g.Parent]; !ok {
			return nil, fmt.Errorf("%w: group %q references unknown parent %q", ErrInvalidArgument, g.ID, g.Parent)
		}
		children[g.Parent] = append(children[g.Parent], g.ID)
	}

	// 从根遍历计算深度，同时发现不可达节点与任何成环边（子节点被二次
	// 访问意味着两个父节点或回边）。
	depth := map[string]int{rootID: 1}
	queue := []string{rootID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		d := depth[id]
		if d > MaxTreeDepth {
			return nil, fmt.Errorf("%w: group %q depth %d exceeds max %d", ErrLimitExceeded, id, d, MaxTreeDepth)
		}
		for _, c := range children[id] {
			if _, seen := depth[c]; seen {
				return nil, fmt.Errorf("%w: group %q has multiple parents or forms a cycle", ErrInvalidArgument, c)
			}
			depth[c] = d + 1
			queue = append(queue, c)
		}
	}
	if len(depth) != len(nodes) {
		return nil, fmt.Errorf("%w: group tree is not a single connected tree", ErrInvalidArgument)
	}

	return &Scheduler{
		groups: nodes,
		jobs:   make(map[string]*jobState),
	}, nil
}

// validateJobSpec 校验作业规格本身。
func (s *Scheduler) validateJobSpec(j JobSpec) error {
	if j.ID == "" {
		return fmt.Errorf("%w: empty job id", ErrInvalidArgument)
	}
	if j.Work <= 0 {
		return fmt.Errorf("%w: job %q work must be positive, got %d", ErrInvalidArgument, j.ID, j.Work)
	}
	if _, ok := s.groups[j.Group]; !ok {
		return fmt.Errorf("%w: job %q references unknown group %q", ErrInvalidArgument, j.ID, j.Group)
	}
	if j.ReleaseAt > j.Deadline {
		return fmt.Errorf("%w: job %q release %d after deadline %d", ErrInvalidArgument, j.ID, j.ReleaseAt, j.Deadline)
	}
	return nil
}

// ancestorPath 返回从 gid 到根的组 ID 序列（含 gid 与根）。
func (s *Scheduler) ancestorPath(gid string) []string {
	path := []string{gid}
	cur := gid
	for s.groups[cur].spec.Parent != "" {
		cur = s.groups[cur].spec.Parent
		path = append(path, cur)
	}
	return path
}

// effectiveQuota 返回某个组当前状态下的本周期有效额度。
func (s *Scheduler) effectiveQuota(g *groupNode) int {
	return g.spec.Quota + g.adjust
}

// feasible 判断沿 path 的所有祖先在当前周期是否仍有有效额度。
func (s *Scheduler) feasible(path []string) bool {
	for _, gid := range path {
		g := s.groups[gid]
		if g.used >= s.effectiveQuota(g) {
			return false
		}
	}
	return true
}

// nearestExhausted 返回 path 上最深（离作业所在组最近）的有效额度耗尽
// 祖先；没有则返回空串。
func (s *Scheduler) nearestExhausted(path []string) string {
	for _, gid := range path {
		g := s.groups[gid]
		if g.used >= s.effectiveQuota(g) {
			return gid
		}
	}
	return ""
}

// projectedLocked 投影“下一个进入 tick 开头完成周期重置与待入账转让后”
// 的本周期状态，返回 组 -> (有效额度, 已用量)：
//   - 若下一个 tick 是周期边界（now%PeriodTicks == 0），上一周期的已用量
//     与转让净额全部归零，待入账转让作为新周期的首批调整生效；
//   - 否则保持当前周期状态，再叠加待入账转让。
//
// 没有待入账转让且不在边界时，投影就是实时状态本身，保证不发生转让时
// 对外可见行为与原来完全一致。
func (s *Scheduler) projectedLocked() map[string]struct{ effective, used int } {
	out := make(map[string]struct{ effective, used int }, len(s.groups))
	boundary := s.now%PeriodTicks == 0
	for id, g := range s.groups {
		adjust, used := g.adjust, g.used
		if boundary {
			adjust, used = 0, 0
		}
		out[id] = struct{ effective, used int }{g.spec.Quota + adjust, used}
	}
	for _, c := range s.pending {
		if c.kind != ChangeTransfer {
			continue
		}
		donor := out[c.from]
		donor.effective -= c.amount
		out[c.from] = donor
		recv := out[c.group]
		recv.effective += c.amount
		out[c.group] = recv
	}
	return out
}

// readyJobIDs 返回当前 tick 下按（截止 tick，作业 ID）排序的就绪作业：
// 已释放、剩余工作量大于 0、未取消。已完成的作业不在其中。
func (s *Scheduler) readyJobIDs(tick uint64) []string {
	ids := make([]string, 0)
	for id, j := range s.jobs {
		if j.state != JobReady {
			continue
		}
		if j.remaining <= 0 {
			continue
		}
		if j.spec.ReleaseAt > tick {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool {
		ja, jb := s.jobs[ids[a]], s.jobs[ids[b]]
		if ja.spec.Deadline != jb.spec.Deadline {
			return ja.spec.Deadline < jb.spec.Deadline
		}
		return ids[a] < ids[b]
	})
	return ids
}

// Submit 提交一个作业。expectedRevision 必须等于当前修订号。
// 提交立即对状态生效，并在下一个 tick 开头作为 SUBMIT 变更被处理；
// 若提交发生在 tick t 推进之前，则该作业可以在 tick t 参与调度。
func (s *Scheduler) Submit(j JobSpec, expectedRevision uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitLocked(j, expectedRevision)
}

func (s *Scheduler) submitLocked(j JobSpec, expectedRevision uint64) (CommitResult, error) {
	if err := s.validateJobSpec(j); err != nil {
		return CommitResult{}, err
	}
	if s.revision != expectedRevision {
		return CommitResult{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, expectedRevision, s.revision)
	}
	if _, exists := s.jobs[j.ID]; exists {
		return CommitResult{}, fmt.Errorf("%w: job %q already exists", ErrInvalidArgument, j.ID)
	}
	if len(s.jobs) >= MaxJobs {
		return CommitResult{}, fmt.Errorf("%w: %d jobs exceeds max %d", ErrLimitExceeded, len(s.jobs)+1, MaxJobs)
	}

	s.revision++
	rev := s.revision
	s.jobs[j.ID] = &jobState{
		spec:      j,
		group:     j.Group,
		remaining: j.Work,
		state:     JobReady,
	}
	s.pending = append(s.pending, committedChange{
		kind: ChangeSubmit, jobID: j.ID, group: j.Group, revision: rev,
	})
	return CommitResult{Revision: rev, Now: s.now}, nil
}

// Migrate 将作业迁移到目标组。expectedRevision 必须等于当前修订号。
// 迁移不退还该作业此前在旧祖先路径上消耗的任何配额；迁移同样立即生效，
// 并在下一个 tick 开头作为 MIGRATE 变更被处理。已完成或已取消的作业
// 不可迁移，同组迁移也会被拒绝。
func (s *Scheduler) Migrate(jobID, targetGroup string, expectedRevision uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.migrateLocked(jobID, targetGroup, expectedRevision)
}

func (s *Scheduler) migrateLocked(jobID, targetGroup string, expectedRevision uint64) (CommitResult, error) {
	j, ok := s.jobs[jobID]
	if !ok {
		return CommitResult{}, fmt.Errorf("%w: unknown job %q", ErrNotFound, jobID)
	}
	if _, ok := s.groups[targetGroup]; !ok {
		return CommitResult{}, fmt.Errorf("%w: unknown target group %q", ErrNotFound, targetGroup)
	}
	if j.state != JobReady {
		return CommitResult{}, fmt.Errorf("%w: job %q is %s", ErrJobTerminal, jobID, j.state)
	}
	if j.group == targetGroup {
		return CommitResult{}, fmt.Errorf("%w: job %q already in group %q", ErrInvalidArgument, jobID, targetGroup)
	}
	if s.revision != expectedRevision {
		return CommitResult{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, expectedRevision, s.revision)
	}

	s.revision++
	rev := s.revision
	j.group = targetGroup
	s.pending = append(s.pending, committedChange{
		kind: ChangeMigrate, jobID: jobID, group: targetGroup, revision: rev,
	})
	return CommitResult{Revision: rev, Now: s.now}, nil
}

// Cancel 取消作业。expectedRevision 必须等于当前修订号。取消立即生效，
// 并在下一个 tick 开头作为 CANCEL 变更被处理；已完成或已取消的作业
// 不可再次取消。
func (s *Scheduler) Cancel(jobID string, expectedRevision uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelLocked(jobID, expectedRevision)
}

func (s *Scheduler) cancelLocked(jobID string, expectedRevision uint64) (CommitResult, error) {
	j, ok := s.jobs[jobID]
	if !ok {
		return CommitResult{}, fmt.Errorf("%w: unknown job %q", ErrNotFound, jobID)
	}
	if j.state != JobReady {
		return CommitResult{}, fmt.Errorf("%w: job %q is %s", ErrJobTerminal, jobID, j.state)
	}
	if s.revision != expectedRevision {
		return CommitResult{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, expectedRevision, s.revision)
	}

	s.revision++
	rev := s.revision
	j.state = JobCanceled
	s.pending = append(s.pending, committedChange{
		kind: ChangeCancel, jobID: jobID, revision: rev,
	})
	return CommitResult{Revision: rev, Now: s.now}, nil
}

// Transfer 在两个同父兄弟组之间临时转让本周期的部分配额：amount 从
// donor（捐出组）的本周期有效额度中划出，加到 receiver（受让组）。
// expectedRevision 必须等于当前修订号。
//
// 约束：
//   - donor 与 receiver 都必须存在、不能同组、必须是同父兄弟；根没有
//     兄弟，不能参与；
//   - amount 必须为正，且不超过捐出组“下一个 tick 入账后”本周期的未用
//     额度（有效额度 - 已用量）。若提交发生在周期边界 tick 之前，则按
//     归零后的新周期判定，临界提交不可能用上一周期的余额；
//   - 转让只改变这两个组本周期的有效额度：父组与任何祖先组额度不变，
//     捐出组已执行 tick 的扣减也不退还；
//   - 转让立即提交（修订号 +1），在下一个 tick 开头作为 TRANSFER 变更
//     进入该 tick 的 Applied 轨迹并对调度生效；到周期边界净额归零，
//     两组额度恢复为基础额度。
func (s *Scheduler) Transfer(donor, receiver string, amount int, expectedRevision uint64) (CommitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transferLocked(donor, receiver, amount, expectedRevision)
}

func (s *Scheduler) transferLocked(donor, receiver string, amount int, expectedRevision uint64) (CommitResult, error) {
	dg, ok := s.groups[donor]
	if !ok {
		return CommitResult{}, fmt.Errorf("%w: unknown donor group %q", ErrNotFound, donor)
	}
	rg, ok := s.groups[receiver]
	if !ok {
		return CommitResult{}, fmt.Errorf("%w: unknown receiver group %q", ErrNotFound, receiver)
	}
	if donor == receiver {
		return CommitResult{}, fmt.Errorf("%w: donor and receiver must differ, got %q", ErrInvalidArgument, donor)
	}
	if dg.spec.Parent == "" || rg.spec.Parent == "" || dg.spec.Parent != rg.spec.Parent {
		return CommitResult{}, fmt.Errorf("%w: groups %q and %q are not siblings under the same parent",
			ErrInvalidArgument, donor, receiver)
	}
	if amount <= 0 {
		return CommitResult{}, fmt.Errorf("%w: transfer amount must be positive, got %d", ErrInvalidArgument, amount)
	}
	if s.revision != expectedRevision {
		return CommitResult{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, expectedRevision, s.revision)
	}
	// 按“下一个 tick 入账后”的周期状态判定未用额度：边界处先归零，
	// 临界提交不可能误用上一周期余额。
	proj := s.projectedLocked()
	available := proj[donor].effective - proj[donor].used
	if amount > available {
		return CommitResult{}, fmt.Errorf("%w: transfer %d exceeds donor %q current-cycle unused %d",
			ErrInvalidArgument, amount, donor, available)
	}

	s.revision++
	rev := s.revision
	s.pending = append(s.pending, committedChange{
		kind: ChangeTransfer, from: donor, group: receiver, amount: amount, revision: rev,
	})
	return CommitResult{Revision: rev, Now: s.now}, nil
}

// Advance 推进时钟 ticks 个整数 tick。expectedRevision 必须等于当前修订号，
// 否则整体拒绝、一个 tick 都不执行。
//
// 整个推进在一把锁内线性化：并发的 Advance 要么因修订号冲突被拒绝，
// 要么串行执行互不相交的 tick 区间，因此不可能重复执行同一个 tick。
// 推进期间其他提交/迁组/取消会阻塞到整个推进结束，随后进入下一 tick。
func (s *Scheduler) Advance(ticks int, expectedRevision uint64) (AdvanceResult, error) {
	if ticks <= 0 {
		return AdvanceResult{}, fmt.Errorf("%w: ticks must be positive, got %d", ErrInvalidArgument, ticks)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.revision != expectedRevision {
		return AdvanceResult{}, fmt.Errorf("%w: expected %d, current %d", ErrConflict, expectedRevision, s.revision)
	}

	from := s.now
	res := AdvanceResult{FromTick: from}
	for i := 0; i < ticks; i++ {
		entry, overdue := s.stepLocked(s.now)
		s.now++
		s.revision++
		entry.Tick = s.now - 1
		s.trace = append(s.trace, entry)
		res.Ticks = append(res.Ticks, entry)
		if len(overdue) > 0 {
			s.overdue = append(s.overdue, overdue...)
			res.Overdue = append(res.Overdue, overdue...)
		}
	}
	res.ToTick = s.now
	res.Revision = s.revision
	return res, nil
}

// stepLocked 执行单个 tick 的完整决策（不含时钟推进与修订号递增）。
// 顺序严格为：周期重置 → 处理已提交变更 → 选择并执行作业（或记录空转）
// → 超期判定。
func (s *Scheduler) stepLocked(tick uint64) (TraceEntry, []OverdueEvidence) {
	period := tick / PeriodTicks
	entry := TraceEntry{Tick: tick, Period: period}

	// 1) 周期边界：周期首个 tick 清零所有组的已用配额与本周期转让净额，
	//    有效额度恢复为基础额度。
	if tick%PeriodTicks == 0 {
		for _, g := range s.groups {
			g.used = 0
			g.adjust = 0
		}
	}

	// 2) 处理自上一个 tick 以来已提交的变更（立即生效模型下仅需记录）。
	if len(s.pending) > 0 {
		entry.Applied = make([]AppliedChange, 0, len(s.pending))
		for _, c := range s.pending {
			if c.kind == ChangeTransfer {
				// 转让在本 tick 才对调度生效：只调整两组本周期的有效
				// 额度，不动父组/祖先额度，也不退还任何已执行扣减。
				s.groups[c.from].adjust -= c.amount
				s.groups[c.group].adjust += c.amount
			}
			entry.Applied = append(entry.Applied, AppliedChange{
				Kind: c.kind, JobID: c.jobID, Revision: c.revision, Group: c.group,
				From: c.from, Amount: c.amount,
			})
		}
		s.pending = s.pending[:0]
	}

	// 3) 在“所有祖先仍有配额”的就绪作业中按（截止 tick，作业 ID）选择。
	ready := s.readyJobIDs(tick)
	entry.ReadyCount = len(ready)

	var chosen string
	var chosenPath []string
	for _, id := range ready {
		path := s.ancestorPath(s.jobs[id].group)
		if s.feasible(path) {
			chosen = id
			chosenPath = path
			break
		}
	}

	switch {
	case chosen != "":
		entry.Kind = TickRan
		entry.Ran = true
		entry.JobID = chosen
		entry.Group = s.jobs[chosen].group
		entry.Path = append([]string(nil), chosenPath...)
		for _, gid := range chosenPath {
			s.groups[gid].used++
			entry.Deducted = append(entry.Deducted, s.groups[gid].used)
		}
		j := s.jobs[chosen]
		j.remaining -= WorkPerTick
		if j.remaining <= 0 {
			j.remaining = 0
			j.state = JobCompleted
		}
	case len(ready) == 0:
		entry.Kind = TickIdleNotReady
	default:
		// 存在就绪作业但全部被配额挡住：记录排序后第一个候选的
		// 最深耗尽祖先，作为本 tick 的阻挡祖先。
		entry.Kind = TickIdleBlocked
		first := ready[0]
		path := s.ancestorPath(s.jobs[first].group)
		entry.BlockedBy = s.nearestExhausted(path)
	}

	// 4) 超期证据：tick Deadline 结束时仍有剩余工作量的就绪作业即超期。
	// 证据随截止 tick 的轨迹产出（DetectedAt == Deadline）；同一作业只
	// 记录一次，已完成/已取消的作业不产生证据。按作业 ID 排序输出，
	// 保证独立参考实现可以逐字段复现。
	var overdue []OverdueEvidence
	var ids []string
	for id, j := range s.jobs {
		if j.state == JobReady && j.remaining > 0 && tick == j.spec.Deadline && !j.overdue {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		j := s.jobs[id]
		j.overdue = true
		path := s.ancestorPath(j.group)
		overdue = append(overdue, OverdueEvidence{
			JobID:      id,
			Deadline:   j.spec.Deadline,
			DetectedAt: j.spec.Deadline,
			Group:      j.group,
			Remaining:  j.remaining,
			State:      j.state,
			Blocker:    s.nearestExhausted(path),
		})
	}

	return entry, overdue
}

// Snapshot 返回当前状态的深拷贝快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups := make([]GroupSpec, 0, len(s.groups))
	for _, g := range s.groups {
		groups = append(groups, g.spec)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })

	jobs := make([]JobView, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, JobView{
			ID:        j.spec.ID,
			Group:     j.group,
			ReleaseAt: j.spec.ReleaseAt,
			Deadline:  j.spec.Deadline,
			Work:      j.spec.Work,
			Remaining: j.remaining,
			State:     j.state,
			Overdue:   j.overdue,
		})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })

	period := s.now / PeriodTicks
	proj := s.projectedLocked()
	quotas := make([]QuotaView, 0, len(s.groups))
	for _, g := range s.groups {
		p := proj[g.spec.ID]
		quotas = append(quotas, QuotaView{
			GroupID: g.spec.ID, Period: period,
			Quota: g.spec.Quota, Effective: p.effective, Used: p.used,
		})
	}
	sort.Slice(quotas, func(i, j int) bool { return quotas[i].GroupID < quotas[j].GroupID })

	return Snapshot{
		Now:      s.now,
		Revision: s.revision,
		Groups:   groups,
		Jobs:     jobs,
		Quotas:   quotas,
	}
}

// Trace 返回从启动至今逐 tick 轨迹的深拷贝。
func (s *Scheduler) Trace() []TraceEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneTrace(s.trace)
}

// Overdue 返回全部已记录超期证据的深拷贝（按检测 tick、作业 ID 排序）。
func (s *Scheduler) Overdue() []OverdueEvidence {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OverdueEvidence, len(s.overdue))
	copy(out, s.overdue)
	return out
}

// Now 返回当前时钟（已执行的 tick 数）。
func (s *Scheduler) Now() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// Revision 返回当前修订号。
func (s *Scheduler) Revision() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}

// cloneTrace 深拷贝轨迹切片（Applied/Path/Deducted 均为值切片，逐元素复制即可）。
func cloneTrace(in []TraceEntry) []TraceEntry {
	if in == nil {
		return nil
	}
	out := make([]TraceEntry, len(in))
	for i, e := range in {
		cp := e
		if e.Applied != nil {
			cp.Applied = append([]AppliedChange(nil), e.Applied...)
		}
		if e.Path != nil {
			cp.Path = append([]string(nil), e.Path...)
		}
		if e.Deducted != nil {
			cp.Deducted = append([]int(nil), e.Deducted...)
		}
		out[i] = cp
	}
	return out
}

// RenderTrace 将轨迹渲染为便于核对的逐 tick 文本。
func RenderTrace(trace []TraceEntry) string {
	var b strings.Builder
	for _, e := range trace {
		fmt.Fprintf(&b, "tick=%d period=%d kind=%s", e.Tick, e.Period, e.Kind)
		if len(e.Applied) > 0 {
			parts := make([]string, 0, len(e.Applied))
			for _, c := range e.Applied {
				switch c.Kind {
				case ChangeSubmit:
					parts = append(parts, fmt.Sprintf("SUBMIT[%s->%s]@r%d", c.JobID, c.Group, c.Revision))
				case ChangeMigrate:
					parts = append(parts, fmt.Sprintf("MIGRATE[%s->%s]@r%d", c.JobID, c.Group, c.Revision))
				case ChangeTransfer:
					parts = append(parts, fmt.Sprintf("TRANSFER[%s->%s x%d]@r%d", blankAsDash(c.From), c.Group, c.Amount, c.Revision))
				default:
					parts = append(parts, fmt.Sprintf("CANCEL[%s]@r%d", c.JobID, c.Revision))
				}
			}
			fmt.Fprintf(&b, " applied=%s", strings.Join(parts, ","))
		}
		switch e.Kind {
		case TickRan:
			fmt.Fprintf(&b, " job=%s group=%s path=%s used=%v", e.JobID, e.Group, strings.Join(e.Path, ">"), e.Deducted)
		case TickIdleBlocked:
			fmt.Fprintf(&b, " blockedBy=%s ready=%d", e.BlockedBy, e.ReadyCount)
		default:
			fmt.Fprintf(&b, " ready=%d", e.ReadyCount)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// RenderOverdue 将超期证据渲染为文本。
func RenderOverdue(evidence []OverdueEvidence) string {
	var b strings.Builder
	for _, ev := range evidence {
		fmt.Fprintf(&b, "job=%s deadline=%d detectedAt=%d group=%s remaining=%d state=%s blocker=%s\n",
			ev.JobID, ev.Deadline, ev.DetectedAt, ev.Group, ev.Remaining, ev.State, blankAsDash(ev.Blocker))
	}
	return b.String()
}

func blankAsDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
