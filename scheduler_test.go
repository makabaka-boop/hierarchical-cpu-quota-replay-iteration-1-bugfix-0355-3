package tenantsched_test

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"tenantsched"
)

// ---------- 测试脚本：同一脚本同时驱动产品与参考实现 ----------

type opKind int

const (
	opSubmit opKind = iota
	opMigrate
	opCancel
	opTransfer
	opAdvance
)

type scriptOp struct {
	kind opKind

	job    tenantsched.JobSpec // submit
	target string              // migrate
	jobID  string              // migrate/cancel
	ticks  int                 // advance

	// transfer：donor -> receiver 转 amount
	donor    string
	receiver string
	amount   int

	// wantErrIs 非 nil 时预期操作失败并匹配该哨兵错误。
	wantErrIs error
}

type scenario struct {
	name   string
	groups []tenantsched.GroupSpec
	script []scriptOp
}

// normalizeTrace 抹掉参考实现不维护的字段（提交修订号在脚本层另行核对）。
func normalizeTrace(in []tenantsched.TraceEntry) []tenantsched.TraceEntry {
	out := make([]tenantsched.TraceEntry, len(in))
	for i, e := range in {
		cp := e
		cp.Applied = nil
		if len(e.Applied) > 0 {
			cp.Applied = make([]tenantsched.AppliedChange, len(e.Applied))
			for k, a := range e.Applied {
				cp.Applied[k] = tenantsched.AppliedChange{
					Kind: a.Kind, JobID: a.JobID, Group: a.Group,
					From: a.From, Amount: a.Amount,
				}
			}
		}
		out[i] = cp
	}
	return out
}

// appliedRevisions 从产品轨迹中抽出每个 tick 处理变更时的修订号序列。
func appliedRevisions(trace []tenantsched.TraceEntry) [][]uint64 {
	var out [][]uint64
	for _, e := range trace {
		var revs []uint64
		for _, a := range e.Applied {
			revs = append(revs, a.Revision)
		}
		out = append(out, revs)
	}
	return out
}

// normalizeOverdue 统一 nil/空切片差异，使 DeepEqual 只比较内容。
func normalizeOverdue(in []tenantsched.OverdueEvidence) []tenantsched.OverdueEvidence {
	if in == nil {
		return []tenantsched.OverdueEvidence{}
	}
	return in
}

func runScenario(t *testing.T, sc scenario) {
	t.Helper()

	prod, err := tenantsched.New(sc.groups)
	if err != nil {
		t.Fatalf("product New: %v", err)
	}
	ref, err := newRefModel(sc.groups)
	if err != nil {
		t.Fatalf("reference New: %v", err)
	}

	var commitRevs []uint64 // 产品侧成功提交变更的修订号，按提交顺序

	for idx, op := range sc.script {
		switch op.kind {
		case opSubmit:
			pr, perr := prod.Submit(op.job, prod.Revision())
			rr, rerr := ref.submit(op.job, ref.revision)
			checkOp(t, idx, "submit", perr, rerr, op.wantErrIs)
			if perr == nil {
				if pr.Revision != rr {
					t.Fatalf("op %d submit revision mismatch product=%d ref=%d", idx, pr.Revision, rr)
				}
				commitRevs = append(commitRevs, pr.Revision)
			}
		case opMigrate:
			pr, perr := prod.Migrate(op.jobID, op.target, prod.Revision())
			rr, rerr := ref.migrate(op.jobID, op.target, ref.revision)
			checkOp(t, idx, "migrate", perr, rerr, op.wantErrIs)
			if perr == nil {
				if pr.Revision != rr {
					t.Fatalf("op %d migrate revision mismatch product=%d ref=%d", idx, pr.Revision, rr)
				}
				commitRevs = append(commitRevs, pr.Revision)
			}
		case opCancel:
			pr, perr := prod.Cancel(op.jobID, prod.Revision())
			rr, rerr := ref.cancel(op.jobID, ref.revision)
			checkOp(t, idx, "cancel", perr, rerr, op.wantErrIs)
			if perr == nil {
				if pr.Revision != rr {
					t.Fatalf("op %d cancel revision mismatch product=%d ref=%d", idx, pr.Revision, rr)
				}
				commitRevs = append(commitRevs, pr.Revision)
			}
		case opTransfer:
			pr, perr := prod.Transfer(op.donor, op.receiver, op.amount, prod.Revision())
			rr, rerr := ref.transfer(op.donor, op.receiver, op.amount, ref.revision)
			checkOp(t, idx, "transfer", perr, rerr, op.wantErrIs)
			if perr == nil {
				if pr.Revision != rr {
					t.Fatalf("op %d transfer revision mismatch product=%d ref=%d", idx, pr.Revision, rr)
				}
				commitRevs = append(commitRevs, pr.Revision)
			}
		case opAdvance:
			pa, perr := prod.Advance(op.ticks, prod.Revision())
			ra, rerr := ref.advance(op.ticks, ref.revision)
			checkOp(t, idx, "advance", perr, rerr, op.wantErrIs)
			if perr == nil {
				if pa.Revision != ra.Revision || pa.FromTick != ra.FromTick || pa.ToTick != ra.ToTick {
					t.Fatalf("op %d advance envelope mismatch: %+v vs %+v", idx, pa, ra)
				}
				pn, rn := normalizeTrace(pa.Ticks), normalizeTrace(ra.Ticks)
				if !reflect.DeepEqual(pn, rn) {
					t.Fatalf("op %d tick trace mismatch:\nproduct:\n%sreference:\n%s",
						idx, tenantsched.RenderTrace(pa.Ticks), tenantsched.RenderTrace(ra.Ticks))
				}
				if !reflect.DeepEqual(normalizeOverdue(pa.Overdue), normalizeOverdue(ra.Overdue)) {
					t.Fatalf("op %d overdue mismatch:\nproduct:\n%sreference:\n%s",
						idx, tenantsched.RenderOverdue(pa.Overdue), tenantsched.RenderOverdue(ra.Overdue))
				}
			}
		}
	}

	// 全量轨迹、超期证据与最终状态逐字段比对。
	pt, rt := normalizeTrace(prod.Trace()), normalizeTrace(ref.trace)
	if !reflect.DeepEqual(pt, rt) {
		t.Fatalf("full trace mismatch:\nproduct:\n%sreference:\n%s",
			tenantsched.RenderTrace(prod.Trace()), tenantsched.RenderTrace(ref.trace))
	}
	if !reflect.DeepEqual(normalizeOverdue(prod.Overdue()), normalizeOverdue(ref.overdue)) {
		t.Fatalf("full overdue mismatch:\nproduct:\n%sreference:\n%s",
			tenantsched.RenderOverdue(prod.Overdue()), tenantsched.RenderOverdue(ref.overdue))
	}
	if prod.Revision() != ref.revision || prod.Now() != ref.now {
		t.Fatalf("final now/revision mismatch product=(%d,%d) ref=(%d,%d)",
			prod.Now(), prod.Revision(), ref.now, ref.revision)
	}
	compareSnapshots(t, prod.Snapshot(), ref)

	// 产品轨迹中变更修订号必须恰好是各次成功提交返回的修订号，且按 tick
	// 单调出现、每个 tick 内严格递增。
	var flattened []uint64
	var prev uint64
	first := true
	for ti, revs := range appliedRevisions(prod.Trace()) {
		for _, r := range revs {
			if !first && r <= prev {
				t.Fatalf("applied revision not strictly increasing at tick %d: %d <= %d", ti, r, prev)
			}
			prev, first = r, false
			flattened = append(flattened, r)
		}
	}
	if !reflect.DeepEqual(flattened, commitRevs) {
		t.Fatalf("applied revision sequence mismatch: trace=%v commits=%v", flattened, commitRevs)
	}
}

func checkOp(t *testing.T, idx int, name string, perr, rerr error, wantErrIs error) {
	t.Helper()
	if wantErrIs != nil {
		if perr == nil || !errors.Is(perr, wantErrIs) {
			t.Fatalf("op %d %s: product err = %v, want %v", idx, name, perr, wantErrIs)
		}
		// 参考实现对修订号冲突以外的输入错误只要求“都失败”；修订号冲突
		// 必须两边语义一致。
		if errors.Is(wantErrIs, tenantsched.ErrConflict) {
			if rerr == nil || !errors.Is(rerr, tenantsched.ErrConflict) {
				t.Fatalf("op %d %s: reference err = %v, want ErrConflict", idx, name, rerr)
			}
		}
		return
	}
	if perr != nil {
		t.Fatalf("op %d %s: product unexpected error %v", idx, name, perr)
	}
	if rerr != nil {
		t.Fatalf("op %d %s: reference unexpected error %v", idx, name, rerr)
	}
}

func compareSnapshots(t *testing.T, snap tenantsched.Snapshot, ref *refModel) {
	t.Helper()
	if snap.Now != ref.now || snap.Revision != ref.revision {
		t.Fatalf("snapshot now/revision %d/%d vs ref %d/%d", snap.Now, snap.Revision, ref.now, ref.revision)
	}

	var refJobs []tenantsched.JobView
	for id, j := range ref.jobs {
		refJobs = append(refJobs, tenantsched.JobView{
			ID: id, Group: j.group, ReleaseAt: j.releaseAt, Deadline: j.deadline,
			Work: j.work, Remaining: j.remaining, State: j.state, Overdue: j.overdue,
		})
	}
	sort.Slice(refJobs, func(i, j int) bool { return refJobs[i].ID < refJobs[j].ID })
	if !reflect.DeepEqual(snap.Jobs, refJobs) {
		t.Fatalf("snapshot jobs mismatch:\nproduct: %+v\nreference: %+v", snap.Jobs, refJobs)
	}

	var refQuotas []tenantsched.QuotaView
	proj := ref.projected()
	for _, g := range ref.groups {
		p := proj[g.id]
		refQuotas = append(refQuotas, tenantsched.QuotaView{
			GroupID: g.id, Period: ref.now / tenantsched.PeriodTicks,
			Quota: g.quota, Effective: p.effective, Used: p.used,
		})
	}
	sort.Slice(refQuotas, func(i, j int) bool { return refQuotas[i].GroupID < refQuotas[j].GroupID })
	if !reflect.DeepEqual(snap.Quotas, refQuotas) {
		t.Fatalf("snapshot quotas mismatch:\nproduct: %+v\nreference: %+v", snap.Quotas, refQuotas)
	}
}

// ---------- 场景 A：四层祖先耗尽、周期重置与超期证据 ----------

func scenarioAncestorAndPeriod() scenario {
	return scenario{
		name: "ancestor_exhaustion_period_reset_overdue",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 20},
			{ID: "a", Parent: "R", Quota: 10},
			{ID: "b", Parent: "a", Quota: 4},
			{ID: "c", Parent: "b", Quota: 2},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "j1", ReleaseAt: 0, Work: 6, Deadline: 9, Group: "b"}},
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "j4", ReleaseAt: 0, Work: 2, Deadline: 20, Group: "c"}},

			{kind: opAdvance, ticks: 4}, // tick 0..3：j1 连跑 4 次，b 配额耗尽
			{kind: opAdvance, ticks: 1}, // tick 4：IDLE_BLOCKED，阻挡祖先 b
			{kind: opAdvance, ticks: 5}, // tick 5..9：持续被 b 阻挡；tick 9 j1 超期
			{kind: opAdvance, ticks: 1}, // tick 10：周期重置，j1 恢复执行
			{kind: opAdvance, ticks: 1}, // tick 11：j1 完成
			{kind: opAdvance, ticks: 1}, // tick 12：j4 在第 4 层执行，逐级扣减
			{kind: opAdvance, ticks: 8}, // tick 13 j4 完成；14..19 空转
		},
	}
}

// ---------- 场景 B：迁组不退还旧配额，deadline/ID 决胜 ----------

func scenarioMigrateNoRefund() scenario {
	return scenario{
		name: "migrate_no_refund_tiebreak",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "a", Parent: "R", Quota: 1},
			{ID: "b", Parent: "a", Quota: 100},
			{ID: "x", Parent: "R", Quota: 100},
			{ID: "y", Parent: "x", Quota: 100},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jm", ReleaseAt: 0, Work: 3, Deadline: 10, Group: "b"}},
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jn", ReleaseAt: 0, Work: 3, Deadline: 10, Group: "y"}},

			{kind: opAdvance, ticks: 2}, // tick0 jm 跑一次；tick1 旧祖先 a 耗尽挡住 jm
			{kind: opMigrate, jobID: "jm", target: "y"},
			{kind: opAdvance, ticks: 1}, // tick2：jm 在新路径执行；a 已用的 1 不退还
			{kind: opAdvance, ticks: 1}, // tick3：jm 完成
			{kind: opAdvance, ticks: 7}, // tick4..6 jn 完成；tick7..10 空转
		},
	}
}

// ---------- 场景 C：无可运行作业的空转与取消 ----------

func scenarioIdleAndCancel() scenario {
	return scenario{
		name: "idle_not_ready_then_cancel",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jc", ReleaseAt: 5, Work: 2, Deadline: 10, Group: "R"}},
			{kind: opAdvance, ticks: 5}, // tick0..4 无就绪作业
			{kind: opCancel, jobID: "jc"},
			{kind: opAdvance, ticks: 3}, // tick7..9 作业已取消，继续空转
		},
	}
}

// ---------- 场景 E：同父兄弟转让、祖先按有效额度阻挡与超期重放 ----------

func scenarioSiblingTransfer() scenario {
	return scenario{
		name: "sibling_transfer_effective_quota",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 3},
			{ID: "a", Parent: "R", Quota: 4}, // 捐出组：本周期无作业
			{ID: "b", Parent: "R", Quota: 1}, // 受让组
			{ID: "c", Parent: "b", Quota: 10},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jb", ReleaseAt: 0, Work: 4, Deadline: 9, Group: "c"}},

			{kind: opAdvance, ticks: 2}, // tick0 jb 跑一次；tick1 b 基础额度耗尽挡住
			// 超过捐出组未用额度（4）的转让必须被拒，且两侧都不入账。
			{kind: opTransfer, donor: "a", receiver: "b", amount: 5, wantErrIs: tenantsched.ErrInvalidArgument},
			{kind: opTransfer, donor: "a", receiver: "b", amount: 3}, // b 有效额度 1->4

			{kind: opAdvance, ticks: 1}, // tick2：入账 TRANSFER，jb 可跑
			{kind: opAdvance, ticks: 1}, // tick3：再跑一次，祖先 R 用量到 3 耗尽
			{kind: opAdvance, ticks: 1}, // tick4：b 仍有余，但最深阻挡祖先变为 R
			{kind: opAdvance, ticks: 5}, // tick5..9 持续被 R 挡；tick9 jb 超期（欠 1）
			{kind: opAdvance, ticks: 1}, // tick10：周期重置、净额归零，jb 跑完最后 1 并完成
			{kind: opAdvance, ticks: 1}, // tick11：无就绪作业，IDLE_NOT_READY；转让已失效
		},
	}
}

// ---------- 场景 F：转让跨周期失效、边界临界提交按新周期判定 ----------

func scenarioTransferAcrossPeriods() scenario {
	return scenario{
		name: "transfer_expires_at_period_boundary",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "a", Parent: "R", Quota: 2}, // 捐出组：周期 0 内额度被自己作业用满
			{ID: "b", Parent: "R", Quota: 5}, // 受让组
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jd", ReleaseAt: 0, Work: 2, Deadline: 5, Group: "a"}},
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jr", ReleaseAt: 0, Work: 8, Deadline: 19, Group: "b"}},

			{kind: opAdvance, ticks: 2}, // tick0/1 jd 跑完，a 已用 2（不退还）
			{kind: opAdvance, ticks: 5}, // tick2..6 jr 跑 5 次，b 基础额度耗尽
			{kind: opAdvance, ticks: 2}, // tick7/8 jr 被 b 挡住

			// now==9：a 本周期未用额度为 0，任何正转让都必须被拒
			// （已执行 tick 的扣减不因转让退还）。
			{kind: opTransfer, donor: "a", receiver: "b", amount: 1, wantErrIs: tenantsched.ErrInvalidArgument},
			{kind: opAdvance, ticks: 1}, // tick9：继续被 b 挡住

			// now==10，边界 tick 尚未执行：投影先归零，临界提交必须按
			// 新周期满额判定，而不是上周期的零余额。
			{kind: opTransfer, donor: "a", receiver: "b", amount: 2},
			{kind: opAdvance, ticks: 1}, // tick10：重置后入账转让，jr 恢复执行
			{kind: opAdvance, ticks: 2}, // tick11/12：jr 跑完欠的 2 个工作量并完成
			{kind: opAdvance, ticks: 7}, // tick13..19 空转

			// now==20：第二个新周期，上一周期的转让净额早已消失，
			// a 的基础额度恢复后可以再次转出。
			{kind: opTransfer, donor: "a", receiver: "b", amount: 1},
			{kind: opAdvance, ticks: 1}, // tick20：重置后入账新转让，无作业空转
		},
	}
}

// ---------- 场景 G：迁组与转让交织，旧路径扣减不退还 ----------

func scenarioMigrateAndTransfer() scenario {
	return scenario{
		name: "migrate_and_transfer_interleave",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "a", Parent: "R", Quota: 3}, // 捐出组：jm 迁走前在此用掉 2
			{ID: "b", Parent: "R", Quota: 1}, // 受让组
			{ID: "x", Parent: "b", Quota: 10},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jm", ReleaseAt: 0, Work: 5, Deadline: 25, Group: "a"}},

			{kind: opAdvance, ticks: 2}, // tick0/1 jm 在 a 路径跑，a 已用 2
			{kind: opMigrate, jobID: "jm", target: "x"},
			{kind: opAdvance, ticks: 1}, // tick2：入账 MIGRATE，jm 在 x>b>R 跑，b 已用 1
			{kind: opAdvance, ticks: 1}, // tick3：b 耗尽挡住 jm
			// a 未用额度只剩 1（旧扣减不退还）：只能转出 1。
			{kind: opTransfer, donor: "a", receiver: "b", amount: 2, wantErrIs: tenantsched.ErrInvalidArgument},
			{kind: opTransfer, donor: "a", receiver: "b", amount: 1},
			{kind: opAdvance, ticks: 1}, // tick4：入账 TRANSFER，jm 再跑一次
			{kind: opAdvance, ticks: 1}, // tick5：b 有效额度再次耗尽
			{kind: opAdvance, ticks: 4}, // tick6..9：持续被 b 挡住
			{kind: opAdvance, ticks: 1}, // tick10：重置+净额归零，jm 跑完最后 1
			{kind: opAdvance, ticks: 9}, // tick11..19 空转
		},
	}
}

// ---------- 场景 D：同 tick 提交即参与调度 + 批量推进中的超期 ----------

func scenarioSubmitAtTickAndDeadline() scenario {
	return scenario{
		name: "submit_then_advance_same_tick",
		groups: []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "p", Parent: "R", Quota: 2},
		},
		script: []scriptOp{
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "ja", ReleaseAt: 0, Work: 4, Deadline: 3, Group: "p"}},
			{kind: opAdvance, ticks: 4}, // 0,1 执行；2,3 被 p 挡住；tick3 ja 超期
			// tick4 才提交的作业在 tick4 当 tick 即参与调度。
			{kind: opSubmit, job: tenantsched.JobSpec{ID: "jb", ReleaseAt: 4, Work: 1, Deadline: 6, Group: "R"}},
			{kind: opAdvance, ticks: 1},
			{kind: opAdvance, ticks: 5}, // tick6..10：tick9 周期重置；其余空转
		},
	}
}

// TestPendingChangesEnterNextTick 直接核对“先处理已提交变更、再选择作业”
// 的顺序：在两个推进批次之间提交的变更，必须只出现在下一个进入 tick 的
// Applied 列表中，并在同一 tick 立即参与调度。
func TestPendingChangesEnterNextTick(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 10}})
	if err != nil {
		t.Fatal(err)
	}
	first := s.Revision()
	r0, err := s.Advance(2, first) // tick0..1 空转，期间无任何变更
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range r0.Ticks {
		if len(e.Applied) != 0 || e.Kind != tenantsched.TickIdleNotReady {
			t.Fatalf("tick%d should be clean idle, got %+v", i, e)
		}
	}

	// 提交发生在 now==2 时：下一个进入的 tick 是 2。
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "late", ReleaseAt: 2, Work: 1, Deadline: 5, Group: "R",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	r1, err := s.Advance(2, s.Revision()) // tick2 处理提交并执行；tick3 空转
	if err != nil {
		t.Fatal(err)
	}
	if ap := r1.Ticks[0].Applied; len(ap) != 1 ||
		ap[0].Kind != tenantsched.ChangeSubmit || ap[0].JobID != "late" {
		t.Fatalf("tick2 applied = %+v", ap)
	}
	if r1.Ticks[0].Kind != tenantsched.TickRan || r1.Ticks[0].JobID != "late" {
		t.Fatalf("tick2 should run newly submitted job, got %+v", r1.Ticks[0])
	}
	if len(r1.Ticks[1].Applied) != 0 || r1.Ticks[1].Kind != tenantsched.TickIdleNotReady {
		t.Fatalf("tick3 should be clean idle, got %+v", r1.Ticks[1])
	}
}

func TestScenariosAgainstReference(t *testing.T) {
	for _, sc := range []scenario{
		scenarioAncestorAndPeriod(),
		scenarioMigrateNoRefund(),
		scenarioIdleAndCancel(),
		scenarioSubmitAtTickAndDeadline(),
		scenarioSiblingTransfer(),
		scenarioTransferAcrossPeriods(),
		scenarioMigrateAndTransfer(),
	} {
		t.Run(sc.name, func(t *testing.T) { runScenario(t, sc) })
	}
}

// ---------- 修订号乐观并发控制的拒绝语义 ----------

func TestRevisionConflicts(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 5}})
	if err != nil {
		t.Fatal(err)
	}
	j := tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 2, Deadline: 5, Group: "R"}

	// 过期修订号提交。
	if _, err := s.Submit(j, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale submit: %v", err)
	}
	if _, err := s.Advance(1, 7); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale advance: %v", err)
	}
	// 引用不存在的作业时，参数校验先于修订号比较。
	if _, err := s.Migrate("j", "R", 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("migrate missing job: %v", err)
	}
	if _, err := s.Cancel("missing", 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("cancel missing: %v", err)
	}

	if r, err := s.Submit(j, 0); err != nil || r.Revision != 1 {
		t.Fatalf("submit: rev=%v err=%v", r, err)
	}
	// 修订号 0 已过期。
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j2", Work: 1, Deadline: 1, Group: "R"}, 0); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale submit after commit: %v", err)
	}
	// 作业存在性优先于修订号校验，避免陈旧客户端探测状态。
	if _, err := s.Migrate("j", "R", 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("same-group migrate: %v", err)
	}
	if _, err := s.Migrate("j", "nope", 1); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown target migrate: %v", err)
	}

	if _, err := s.Advance(1, 1); err != nil {
		t.Fatalf("advance: %v", err)
	}
	// tick 也会推进修订号：旧修订号 1 的提交必须被拒。
	if _, err := s.Cancel("j", 1); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("cancel after advance with stale rev: %v", err)
	}
	if _, err := s.Cancel("j", 2); err != nil {
		t.Fatalf("cancel with current rev: %v", err)
	}
	if _, err := s.Cancel("j", 3); !errors.Is(err, tenantsched.ErrJobTerminal) {
		t.Fatalf("double cancel: %v", err)
	}
}

// ---------- 结构与参数校验 ----------

func TestValidation(t *testing.T) {
	cases := []struct {
		name   string
		groups []tenantsched.GroupSpec
		want   error
	}{
		{"empty", nil, tenantsched.ErrInvalidArgument},
		{"two roots", []tenantsched.GroupSpec{
			{ID: "R1", Quota: 1}, {ID: "R2", Quota: 1},
		}, tenantsched.ErrInvalidArgument},
		{"dangling parent", []tenantsched.GroupSpec{
			{ID: "R", Quota: 1}, {ID: "a", Parent: "ghost", Quota: 1},
		}, tenantsched.ErrInvalidArgument},
		{"cycle among non-root", []tenantsched.GroupSpec{
			{ID: "R", Quota: 1},
			{ID: "a", Parent: "b", Quota: 1},
			{ID: "b", Parent: "a", Quota: 1},
		}, tenantsched.ErrInvalidArgument},
		{"duplicate ids", []tenantsched.GroupSpec{
			{ID: "R", Quota: 1}, {ID: "R", Parent: "R", Quota: 1},
		}, tenantsched.ErrInvalidArgument},
		{"zero quota", []tenantsched.GroupSpec{{ID: "R", Quota: 0}}, tenantsched.ErrInvalidArgument},
		{"too many groups", buildGroups(21), tenantsched.ErrLimitExceeded},
		{"too deep", deepGroups(5), tenantsched.ErrLimitExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tenantsched.New(tc.groups); !errors.Is(err, tc.want) {
				t.Fatalf("New err = %v, want %v", err, tc.want)
			}
		})
	}

	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1}})
	if err != nil {
		t.Fatal(err)
	}
	bad := []tenantsched.JobSpec{
		{ID: "", Work: 1, Deadline: 1, Group: "R"},
		{ID: "x", Work: 0, Deadline: 1, Group: "R"},
		{ID: "x", Work: 1, Deadline: 1, Group: "ghost"},
		{ID: "x", ReleaseAt: 5, Work: 1, Deadline: 4, Group: "R"},
	}
	for i, j := range bad {
		if _, err := s.Submit(j, uint64(i)); !errors.Is(err, tenantsched.ErrInvalidArgument) &&
			!errors.Is(err, tenantsched.ErrConflict) {
			// 修订号随每次失败不变化；部分用例可能先撞上修订号，两类都允许，
			// 但绝不允许静默成功。
			t.Fatalf("bad spec %d accepted? err=%v", i, err)
		}
	}

	if _, err := s.Advance(0, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("advance zero: %v", err)
	}
}

func TestJobLimit(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < tenantsched.MaxJobs; i++ {
		id := fmt.Sprintf("job%02d", i)
		if _, err := s.Submit(tenantsched.JobSpec{ID: id, Work: 1, Deadline: 1000, Group: "R"}, s.Revision()); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	_, err = s.Submit(tenantsched.JobSpec{ID: "one-too-many", Work: 1, Deadline: 1000, Group: "R"}, s.Revision())
	if !errors.Is(err, tenantsched.ErrLimitExceeded) {
		t.Fatalf("41st job: %v", err)
	}
}

func buildGroups(n int) []tenantsched.GroupSpec {
	out := []tenantsched.GroupSpec{{ID: "R", Quota: 1}}
	for i := 1; i < n; i++ {
		out = append(out, tenantsched.GroupSpec{
			ID: fmt.Sprintf("g%d", i), Parent: "R", Quota: 1,
		})
	}
	return out
}

func deepGroups(depth int) []tenantsched.GroupSpec {
	out := []tenantsched.GroupSpec{{ID: "g1", Quota: 1}}
	for i := 2; i <= depth; i++ {
		out = append(out, tenantsched.GroupSpec{
			ID: fmt.Sprintf("g%d", i), Parent: fmt.Sprintf("g%d", i-1), Quota: 1,
		})
	}
	return out
}
