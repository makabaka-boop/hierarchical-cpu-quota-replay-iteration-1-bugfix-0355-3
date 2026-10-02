package tenantsched_test

import (
	"fmt"
	"sort"

	"tenantsched"
)

// 本文件给出一个与产品实现完全独立的逐 tick 参考调度器：
// 它使用自己的数据模型（map 组结构、独立的作业记录）与自己的步进
// 函数 refStep，只在输出端把结果映射成 tenantsched.TraceEntry /
// OverdueEvidence，供测试逐字段核对产品行为。
//
// 参考模型严格按自然语言规格重写，不调用也不查看 Scheduler 的任何
// 内部辅助函数。

type refGroup struct {
	id     string
	parent string
	quota  int // 基础额度
	used   int
	adjust int // 本周期兄弟组转让净额：有效额度 = quota + adjust
}

type refJob struct {
	id        string
	group     string
	releaseAt uint64
	deadline  uint64
	work      int
	remaining int
	state     tenantsched.JobState
	overdue   bool
}

type refPending struct {
	kind  tenantsched.ChangeKind
	jobID string
	group string

	// 仅 TRANSFER：from 捐出组、amount 转让量（group 复用为受让组）。
	from   string
	amount int
}

type refModel struct {
	now      uint64
	revision uint64

	groups map[string]*refGroup
	jobs   map[string]*refJob
	queue  []refPending // 已提交、等待下一个 tick 开头处理

	trace   []tenantsched.TraceEntry
	overdue []tenantsched.OverdueEvidence
}

func newRefModel(groups []tenantsched.GroupSpec) (*refModel, error) {
	m := &refModel{
		groups: map[string]*refGroup{},
		jobs:   map[string]*refJob{},
	}
	roots := 0
	for _, g := range groups {
		if g.ID == "" || g.Quota <= 0 {
			return nil, fmt.Errorf("bad group spec")
		}
		if _, ok := m.groups[g.ID]; ok {
			return nil, fmt.Errorf("dup group")
		}
		if g.Parent == "" {
			roots++
		}
		m.groups[g.ID] = &refGroup{id: g.ID, parent: g.Parent, quota: g.Quota}
	}
	if roots != 1 {
		return nil, fmt.Errorf("roots=%d", roots)
	}
	// 父引用与可达性。
	for _, g := range m.groups {
		if g.parent != "" {
			if _, ok := m.groups[g.parent]; !ok {
				return nil, fmt.Errorf("dangling parent")
			}
		}
	}
	seen := map[string]bool{}
	var stack []string
	for _, g := range m.groups {
		if g.parent == "" {
			stack = append(stack, g.id)
		}
	}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[id] {
			return nil, fmt.Errorf("cycle")
		}
		seen[id] = true
		for _, g := range m.groups {
			if g.parent == id {
				stack = append(stack, g.id)
			}
		}
	}
	if len(seen) != len(m.groups) {
		return nil, fmt.Errorf("disconnected")
	}
	return m, nil
}

func (m *refModel) pathOf(gid string) []string {
	out := []string{gid}
	cur := gid
	for m.groups[cur].parent != "" {
		cur = m.groups[cur].parent
		out = append(out, cur)
	}
	return out
}

func (m *refModel) effective(g *refGroup) int { return g.quota + g.adjust }

func (m *refModel) canRun(path []string) bool {
	for _, gid := range path {
		g := m.groups[gid]
		if g.used >= m.effective(g) {
			return false
		}
	}
	return true
}

func (m *refModel) blocker(path []string) string {
	for _, gid := range path {
		g := m.groups[gid]
		if g.used >= m.effective(g) {
			return gid
		}
	}
	return ""
}

// projected 独立重写“下一 tick 入账后”的周期投影：边界 tick 前先归零
// 基础状态，再叠加待入账转让；其余 tick 在当前周期上叠加待入账转让。
func (m *refModel) projected() map[string]struct{ effective, used int } {
	out := map[string]struct{ effective, used int }{}
	boundary := m.now%tenantsched.PeriodTicks == 0
	for id, g := range m.groups {
		adjust, used := g.adjust, g.used
		if boundary {
			adjust, used = 0, 0
		}
		out[id] = struct{ effective, used int }{g.quota + adjust, used}
	}
	for _, c := range m.queue {
		if c.kind != tenantsched.ChangeTransfer {
			continue
		}
		d := out[c.from]
		d.effective -= c.amount
		out[c.from] = d
		r := out[c.group]
		r.effective += c.amount
		out[c.group] = r
	}
	return out
}

func (m *refModel) readyAt(t uint64) []string {
	var ids []string
	for id, j := range m.jobs {
		if j.state == tenantsched.JobReady && j.remaining > 0 && j.releaseAt <= t {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(a, b int) bool {
		x, y := m.jobs[ids[a]], m.jobs[ids[b]]
		if x.deadline != y.deadline {
			return x.deadline < y.deadline
		}
		return ids[a] < ids[b]
	})
	return ids
}

func (m *refModel) submit(j tenantsched.JobSpec, expected uint64) (uint64, error) {
	if j.ID == "" || j.Work <= 0 || m.groups[j.Group] == nil || j.ReleaseAt > j.Deadline {
		return 0, fmt.Errorf("bad job")
	}
	if m.jobs[j.ID] != nil {
		return 0, fmt.Errorf("dup job")
	}
	if len(m.jobs) >= tenantsched.MaxJobs {
		return 0, fmt.Errorf("too many jobs")
	}
	if m.revision != expected {
		return 0, tenantsched.ErrConflict
	}
	m.revision++
	m.jobs[j.ID] = &refJob{
		id: j.ID, group: j.Group, releaseAt: j.ReleaseAt, deadline: j.Deadline,
		work: j.Work, remaining: j.Work, state: tenantsched.JobReady,
	}
	m.queue = append(m.queue, refPending{kind: tenantsched.ChangeSubmit, jobID: j.ID, group: j.Group})
	return m.revision, nil
}

func (m *refModel) migrate(jobID, target string, expected uint64) (uint64, error) {
	j := m.jobs[jobID]
	if j == nil || m.groups[target] == nil {
		return 0, tenantsched.ErrNotFound
	}
	if j.state != tenantsched.JobReady {
		return 0, tenantsched.ErrJobTerminal
	}
	if j.group == target {
		return 0, fmt.Errorf("same group")
	}
	if m.revision != expected {
		return 0, tenantsched.ErrConflict
	}
	m.revision++
	j.group = target
	m.queue = append(m.queue, refPending{kind: tenantsched.ChangeMigrate, jobID: jobID, group: target})
	return m.revision, nil
}

func (m *refModel) cancel(jobID string, expected uint64) (uint64, error) {
	j := m.jobs[jobID]
	if j == nil {
		return 0, tenantsched.ErrNotFound
	}
	if j.state != tenantsched.JobReady {
		return 0, tenantsched.ErrJobTerminal
	}
	if m.revision != expected {
		return 0, tenantsched.ErrConflict
	}
	m.revision++
	j.state = tenantsched.JobCanceled
	m.queue = append(m.queue, refPending{kind: tenantsched.ChangeCancel, jobID: jobID})
	return m.revision, nil
}

// refTransfer 独立按自然语言规格实现兄弟组配额转让。
func (m *refModel) transfer(donor, receiver string, amount int, expected uint64) (uint64, error) {
	dg := m.groups[donor]
	rg := m.groups[receiver]
	if dg == nil {
		return 0, tenantsched.ErrNotFound
	}
	if rg == nil {
		return 0, tenantsched.ErrNotFound
	}
	if donor == receiver || dg.parent == "" || rg.parent == "" || dg.parent != rg.parent {
		return 0, fmt.Errorf("not siblings")
	}
	if amount <= 0 {
		return 0, fmt.Errorf("bad amount")
	}
	if m.revision != expected {
		return 0, tenantsched.ErrConflict
	}
	proj := m.projected()
	p := proj[donor]
	if amount > p.effective-p.used {
		return 0, fmt.Errorf("exceeds unused")
	}
	m.revision++
	m.queue = append(m.queue, refPending{
		kind: tenantsched.ChangeTransfer, from: donor, group: receiver, amount: amount,
	})
	return m.revision, nil
}

// refStep 是参考实现的单 tick 决策：重置 -> 入账变更 -> 选择执行 -> 超期。
func (m *refModel) refStep(t uint64) (tenantsched.TraceEntry, []tenantsched.OverdueEvidence) {
	e := tenantsched.TraceEntry{Tick: t, Period: t / tenantsched.PeriodTicks}

	if t%tenantsched.PeriodTicks == 0 {
		for _, g := range m.groups {
			g.used = 0
			g.adjust = 0
		}
	}

	if len(m.queue) > 0 {
		e.Applied = make([]tenantsched.AppliedChange, 0, len(m.queue))
		for _, c := range m.queue {
			if c.kind == tenantsched.ChangeTransfer {
				m.groups[c.from].adjust -= c.amount
				m.groups[c.group].adjust += c.amount
			}
			// 参考实现不依赖修订号字段，统一填 0；比对前会归一化。
			e.Applied = append(e.Applied, tenantsched.AppliedChange{
				Kind: c.kind, JobID: c.jobID, Group: c.group,
				From: c.from, Amount: c.amount,
			})
		}
		m.queue = m.queue[:0]
	}

	ready := m.readyAt(t)
	e.ReadyCount = len(ready)

	var pick string
	var pickPath []string
	for _, id := range ready {
		p := m.pathOf(m.jobs[id].group)
		if m.canRun(p) {
			pick, pickPath = id, p
			break
		}
	}

	switch {
	case pick != "":
		e.Kind = tenantsched.TickRan
		e.Ran = true
		e.JobID = pick
		e.Group = m.jobs[pick].group
		e.Path = append([]string(nil), pickPath...)
		for _, gid := range pickPath {
			m.groups[gid].used++
			e.Deducted = append(e.Deducted, m.groups[gid].used)
		}
		j := m.jobs[pick]
		j.remaining--
		if j.remaining == 0 {
			j.state = tenantsched.JobCompleted
		}
	case len(ready) == 0:
		e.Kind = tenantsched.TickIdleNotReady
	default:
		e.Kind = tenantsched.TickIdleBlocked
		e.BlockedBy = m.blocker(m.pathOf(m.jobs[ready[0]].group))
	}

	var ev []tenantsched.OverdueEvidence
	var ids []string
	for id, j := range m.jobs {
		if j.state == tenantsched.JobReady && j.remaining > 0 && j.deadline == t && !j.overdue {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		j := m.jobs[id]
		j.overdue = true
		ev = append(ev, tenantsched.OverdueEvidence{
			JobID:      id,
			Deadline:   j.deadline,
			DetectedAt: j.deadline,
			Group:      j.group,
			Remaining:  j.remaining,
			State:      j.state,
			Blocker:    m.blocker(m.pathOf(j.group)),
		})
	}
	return e, ev
}

func (m *refModel) advance(n int, expected uint64) (tenantsched.AdvanceResult, error) {
	if n <= 0 {
		return tenantsched.AdvanceResult{}, fmt.Errorf("bad ticks")
	}
	if m.revision != expected {
		return tenantsched.AdvanceResult{}, tenantsched.ErrConflict
	}
	res := tenantsched.AdvanceResult{FromTick: m.now}
	for i := 0; i < n; i++ {
		e, ev := m.refStep(m.now)
		m.now++
		m.revision++
		m.trace = append(m.trace, e)
		res.Ticks = append(res.Ticks, e)
		res.Overdue = append(res.Overdue, ev...)
		m.overdue = append(m.overdue, ev...)
	}
	res.ToTick = m.now
	res.Revision = m.revision
	return res, nil
}
