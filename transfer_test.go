package tenantsched_test

import (
	"errors"
	"testing"

	"tenantsched"
)

// transferGroups 是转让测试的标准三兄弟树：
//
//	R(quota 5)
//	├ a(quota 2)  捐出组
//	├ b(quota 3)  受让组
//	└ c(quota 1)  用于“非同父”等负例
func transferGroups() []tenantsched.GroupSpec {
	return []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 5},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 3},
		{ID: "c", Parent: "R", Quota: 1},
	}
}

func mustNewTransferScheduler(t *testing.T) *tenantsched.Scheduler {
	t.Helper()
	s, err := tenantsched.New(transferGroups())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func quotaByID(qs []tenantsched.QuotaView, id string) tenantsched.QuotaView {
	for _, q := range qs {
		if q.GroupID == id {
			return q
		}
	}
	return tenantsched.QuotaView{GroupID: id}
}

// TestTransferValidation 覆盖转让接口的参数校验、校验顺序与错误哨兵。
func TestTransferValidation(t *testing.T) {
	s := mustNewTransferScheduler(t)

	if _, err := s.Transfer("ghost", "b", 1, 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown donor: %v", err)
	}
	if _, err := s.Transfer("a", "ghost", 1, 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown receiver: %v", err)
	}
	if _, err := s.Transfer("R", "b", 1, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("root donor: %v", err)
	}
	if _, err := s.Transfer("a", "R", 1, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("root receiver: %v", err)
	}
	if _, err := s.Transfer("a", "a", 1, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("same group: %v", err)
	}
	if _, err := s.Transfer("a", "b", 0, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("zero amount: %v", err)
	}
	if _, err := s.Transfer("a", "b", -2, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("negative amount: %v", err)
	}
	// 静态参数校验先于修订号比较：陈旧客户端无法借参数错误探测状态。
	if _, err := s.Transfer("a", "a", 1, 99); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("invalid args before revision: %v", err)
	}
	// 合法参数 + 陈旧修订号 -> 冲突，无副作用。
	if _, err := s.Transfer("a", "b", 1, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale revision: %v", err)
	}

	// 全额转出（未用 = 基础额度 2）合法；修订号 +1。
	r, err := s.Transfer("a", "b", 2, 0)
	if err != nil {
		t.Fatalf("transfer full unused: %v", err)
	}
	if r.Revision != 1 {
		t.Fatalf("revision after transfer = %d, want 1", r.Revision)
	}
	// 旧修订号 0 立即失效。
	if _, err := s.Transfer("a", "c", 1, 0); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("transfer after commit with stale rev: %v", err)
	}
	// 第二笔在同 tick 之前提交：投影中 a 的有效额度已经是 0，
	// 再转出任何正量都必须被拒（失败不入 pending、轨迹保持原样）。
	if _, err := s.Transfer("a", "c", 1, r.Revision); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("second transfer over projected unused: %v", err)
	}
}

// TestTransferSnapshotShowsBaseEffectiveUsed 核对快照同时展示基础额度、
// 有效额度与已用量：提交即投影、入账后生效、父组额度不变。
func TestTransferSnapshotShowsBaseEffectiveUsed(t *testing.T) {
	s := mustNewTransferScheduler(t)

	// 捐出组 a 先消耗 1（未用只剩 1）。
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jd", ReleaseAt: 0, Work: 1, Deadline: 9, Group: "a",
	}, 0); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	rev := res.Revision
	check := func(wantAEff, wantAUsed, wantBEff, wantBUsed, wantREff, wantRUsed int) {
		t.Helper()
		snap := s.Snapshot()
		qa, qb, qr := quotaByID(snap.Quotas, "a"), quotaByID(snap.Quotas, "b"), quotaByID(snap.Quotas, "R")
		if qa.Quota != 2 || qa.Effective != wantAEff || qa.Used != wantAUsed {
			t.Fatalf("a quota/base/eff/used = 2/%d/%d, want 2/%d/%d", qa.Effective, qa.Used, wantAEff, wantAUsed)
		}
		if qb.Quota != 3 || qb.Effective != wantBEff || qb.Used != wantBUsed {
			t.Fatalf("b base/eff/used = 3/%d/%d, want 3/%d/%d", qb.Effective, qb.Used, wantBEff, wantBUsed)
		}
		if qr.Quota != 5 || qr.Effective != wantREff || qr.Used != wantRUsed {
			t.Fatalf("R base/eff/used = 5/%d/%d, want 5/%d/%d (ancestor must be untouched)",
				qr.Effective, qr.Used, wantREff, wantRUsed)
		}
	}
	check(2, 1, 3, 0, 5, 1) // 转让前

	// 提交 1 的转让（此时尚未入账）：快照投影立即显示有效额度变化。
	tr, err := s.Transfer("a", "b", 1, rev)
	if err != nil {
		t.Fatal(err)
	}
	check(1, 1, 4, 0, 5, 1)

	// 入账 tick：有效额度真正对调度生效，快照与投影一致。
	adv, err := s.Advance(1, tr.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if ap := adv.Ticks[0].Applied; len(ap) != 1 || ap[0].Kind != tenantsched.ChangeTransfer ||
		ap[0].From != "a" || ap[0].Group != "b" || ap[0].Amount != 1 {
		t.Fatalf("tick1 applied transfer = %+v", ap)
	}
	check(1, 1, 4, 0, 5, 1)

	// 推进到下一个周期边界：净额归零，两组有效额度恢复为基础额度，
	// 已用量同样归零。
	if _, err := s.Advance(8, s.Revision()); err != nil { // tick2..9
		t.Fatal(err)
	}
	snap := s.Snapshot()
	qa, qb := quotaByID(snap.Quotas, "a"), quotaByID(snap.Quotas, "b")
	if qa.Effective != qa.Quota || qa.Used != 0 {
		t.Fatalf("a after boundary eff=%d base=%d used=%d, want reset", qa.Effective, qa.Quota, qa.Used)
	}
	if qb.Effective != qb.Quota || qb.Used != 0 {
		t.Fatalf("b after boundary eff=%d base=%d used=%d, want reset", qb.Effective, qb.Quota, qb.Used)
	}
}

// TestTransferEffectiveQuotaReplay 构造“按当时有效额度重放阻挡与超期”的
// 场景：b 基础额度 1，转让 3 后可达 4；但祖先 R 只有 3，故第 4 次执行
// 必须被最深祖先 R 挡住，且超期证据的 Blocker 按当时有效额度给出。
func TestTransferEffectiveQuotaReplay(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 3},
		{ID: "a", Parent: "R", Quota: 4},
		{ID: "b", Parent: "R", Quota: 1},
		{ID: "c", Parent: "b", Quota: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jb", ReleaseAt: 0, Work: 4, Deadline: 9, Group: "c",
	}, 0); err != nil {
		t.Fatal(err)
	}
	r1, err := s.Advance(2, s.Revision()) // tick0 跑；tick1 被 b（eff=1）挡住
	if err != nil {
		t.Fatal(err)
	}
	if r1.Ticks[0].Kind != tenantsched.TickRan || r1.Ticks[0].JobID != "jb" {
		t.Fatalf("tick0 = %+v, want RAN jb", r1.Ticks[0])
	}
	if e1 := r1.Ticks[1]; e1.Kind != tenantsched.TickIdleBlocked || e1.BlockedBy != "b" {
		t.Fatalf("tick1 = %+v, want blocked by b", e1)
	}

	tr, err := s.Transfer("a", "b", 3, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Advance(2, tr.Revision) // tick2 入账转让并跑；tick3 再跑，R 用量到 3
	if err != nil {
		t.Fatal(err)
	}
	if e2 := r2.Ticks[0]; e2.Kind != tenantsched.TickRan ||
		len(e2.Applied) != 1 || e2.Applied[0].Kind != tenantsched.ChangeTransfer {
		t.Fatalf("tick2 = %+v, want RAN with TRANSFER applied", e2)
	}
	if e3 := r2.Ticks[1]; e3.Kind != tenantsched.TickRan {
		t.Fatalf("tick3 = %+v, want RAN", e3)
	}

	// b 有效额度此时为 4（已用 3），仍可跑；但 R 已耗尽（3/3）——
	// 最深阻挡祖先必须是 R，证明阻挡按有效额度重放且不增加祖先额度。
	r3, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if e4 := r3.Ticks[0]; e4.Kind != tenantsched.TickIdleBlocked || e4.BlockedBy != "R" {
		t.Fatalf("tick4 = %+v, want blocked by R (effective-quota replay)", e4)
	}

	r4, err := s.Advance(5, s.Revision()) // tick5..9
	if err != nil {
		t.Fatal(err)
	}
	if e9 := r4.Ticks[4]; e9.Kind != tenantsched.TickIdleBlocked || e9.BlockedBy != "R" {
		t.Fatalf("tick9 = %+v, want still blocked by R", e9)
	}
	overdue := r4.Overdue
	if len(overdue) != 1 {
		t.Fatalf("overdue = %+v, want exactly 1 evidence", overdue)
	}
	ev := overdue[0]
	if ev.JobID != "jb" || ev.Remaining != 1 || ev.Group != "c" || ev.Blocker != "R" {
		t.Fatalf("overdue evidence = %+v, want remaining=1 blocker=R", ev)
	}
}

// TestTransferBoundaryCommitUsesFreshCycle 验证“临界提交不能误用上一周期
// 余额”：捐出组在上一周期把基础额度用光后，在边界 tick 之前提交转让，
// 必须按归零后的新周期判定为合法；而非边界位置的同额转让必须被拒。
func TestTransferBoundaryCommitUsesFreshCycle(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	// a 的作业用满 a 的 2 个本周期额度（不退还）。
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jd", ReleaseAt: 0, Work: 2, Deadline: 5, Group: "a",
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(2, s.Revision()); err != nil {
		t.Fatal(err)
	}

	// now=2：本周期 a 未用为 0，正量转让必须被拒。
	if _, err := s.Transfer("a", "b", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("mid-cycle transfer with zero unused: %v", err)
	}
	if _, err := s.Advance(7, s.Revision()); err != nil { // 推进到 now=9
		t.Fatal(err)
	}
	// now=9（边界 tick 前）：上周期余额仍为 0，拒绝。
	if _, err := s.Transfer("a", "b", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("pre-boundary (now=9) transfer must see old-cycle zero balance: %v", err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick9，now=10
		t.Fatal(err)
	}
	// now=10：下一 tick 是边界 tick，投影先归零，必须按新周期满额放行。
	tr, err := s.Transfer("a", "b", 2, s.Revision())
	if err != nil {
		t.Fatalf("boundary commit must use fresh cycle: %v", err)
	}
	snap := s.Snapshot()
	if qa, qb := quotaByID(snap.Quotas, "a"), quotaByID(snap.Quotas, "b"); qa.Effective != 0 || qa.Used != 0 || qb.Effective != 4 {
		t.Fatalf("boundary projection a=%+v b=%+v, want a eff=0/used=0 b eff=4", qa, qb)
	}
	// 入账边界 tick：快照显示的就是重置后的真实状态。
	if _, err := s.Advance(1, tr.Revision); err != nil { // tick10
		t.Fatal(err)
	}
	snap = s.Snapshot()
	if qa, qb := quotaByID(snap.Quotas, "a"), quotaByID(snap.Quotas, "b"); qa.Effective != 0 || qa.Used != 0 || qb.Effective != 4 {
		t.Fatalf("after boundary tick a=%+v b=%+v, want eff 0/4 used 0", qa, qb)
	}
}

// TestTransferChainUsesProjectedEffective 连续转让证明“未用额度”基于投影
// 的有效额度：第一笔把 a 的额度划给 b 后，b 立刻可以把尚未使用的部分再
// 转给 c；而把已执行 tick 的用量一并转出必须被拒。
func TestTransferChainUsesProjectedEffective(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 5},
		{ID: "b", Parent: "R", Quota: 1},
		{ID: "c", Parent: "R", Quota: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	// b 先跑掉 1，已用量不退还也不能再次转出。
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jb", ReleaseAt: 0, Work: 1, Deadline: 9, Group: "b",
	}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil {
		t.Fatal(err)
	}
	tr1, err := s.Transfer("a", "b", 4, s.Revision()) // a:5->1, b:1->5
	if err != nil {
		t.Fatal(err)
	}
	// b 投影有效 5、已用 1，未用 4：可再转出 4 给 c；转 5 必须被拒。
	if _, err := s.Transfer("b", "c", 5, tr1.Revision); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("chained transfer must not move already-used ticks: %v", err)
	}
	tr2, err := s.Transfer("b", "c", 4, tr1.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(1, tr2.Revision); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	qa, qb, qc := quotaByID(snap.Quotas, "a"), quotaByID(snap.Quotas, "b"), quotaByID(snap.Quotas, "c")
	if qa.Effective != 1 {
		t.Fatalf("a effective = %d, want 1", qa.Effective)
	}
	if qb.Effective != 1 || qb.Used != 1 {
		t.Fatalf("b effective/used = %d/%d, want 1/1 (used ticks never refunded)", qb.Effective, qb.Used)
	}
	if qc.Effective != 5 {
		t.Fatalf("c effective = %d, want 5", qc.Effective)
	}

	// 应用序必须是 TRANSFER a->b 然后 b->c，且各自只出现一次。
	trace := s.Trace()
	var last []tenantsched.AppliedChange
	for i := len(trace) - 1; i >= 0; i-- {
		if len(trace[i].Applied) > 0 {
			last = trace[i].Applied
			break
		}
	}
	if len(last) != 2 || last[0].From != "a" || last[0].Group != "b" ||
		last[1].From != "b" || last[1].Group != "c" {
		t.Fatalf("applied order = %+v, want a->b then b->c", last)
	}
}

// TestTransferDoesNotChangeBehaviorWithoutTransfers 保证没有转让时，
// 快照的 Effective 严格等于基础 Quota，且所有既有字段保持原样。
func TestTransferDoesNotChangeBehaviorWithoutTransfers(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 3},
		{ID: "a", Parent: "R", Quota: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "j", ReleaseAt: 0, Work: 5, Deadline: 20, Group: "a",
	}, 0); err != nil {
		t.Fatal(err)
	}
	for step := 0; step < 12; step++ {
		if _, err := s.Advance(1, s.Revision()); err != nil {
			t.Fatal(err)
		}
		for _, q := range s.Snapshot().Quotas {
			if q.Effective != q.Quota {
				t.Fatalf("step %d group %s effective=%d != base=%d without any transfer",
					step, q.GroupID, q.Effective, q.Quota)
			}
		}
	}
	if _, err := s.Advance(1, 1000); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale advance: %v", err)
	}
}

// TestTransferBoundaryWithSubmitSameTick 转让与提交都发生在边界 tick
// 之前：边界 tick 开头必须先重置、再按修订号顺序一并入账；这笔转让是在
// now=10 提交（新周期满额），而不是 now=9（旧周期零余额）。
func TestTransferBoundaryWithSubmitSameTick(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "jd", ReleaseAt: 0, Work: 2, Deadline: 5, Group: "a"}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(10, s.Revision()); err != nil { // tick0..9
		t.Fatal(err)
	}
	// now=10：a 已在旧周期用满，但临界转让按新周期放行。
	tr, err := s.Transfer("a", "b", 2, s.Revision())
	if err != nil {
		t.Fatalf("boundary transfer: %v", err)
	}
	// 同一 pending 批次里再夹一个提交（新作业在 b，需要 b 有效额度 >1）。
	sr, err := s.Submit(tenantsched.JobSpec{ID: "jn", ReleaseAt: 10, Work: 3, Deadline: 15, Group: "b"}, tr.Revision)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(1, sr.Revision)
	if err != nil {
		t.Fatal(err)
	}
	e := res.Ticks[0]
	if e.Tick != 10 || e.Kind != tenantsched.TickRan || e.JobID != "jn" {
		t.Fatalf("tick10 = %+v, want RAN jn after reset+transfer", e)
	}
	if len(e.Applied) != 2 || e.Applied[0].Kind != tenantsched.ChangeTransfer ||
		e.Applied[1].Kind != tenantsched.ChangeSubmit {
		t.Fatalf("tick10 applied order = %+v, want TRANSFER then SUBMIT", e.Applied)
	}
	// 非兄弟在边界同样被拒。
	if _, err := s.Transfer("R", "b", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("root sibling check at boundary: %v", err)
	}
}

// TestTransferAppliedRevision 转让必须作为一次已提交变更进入下一 tick 的
// Applied 轨迹，携带提交成功时的修订号；失败转让绝不进入轨迹。
func TestTransferAppliedRevision(t *testing.T) {
	s := mustNewTransferScheduler(t)
	tr, err := s.Transfer("a", "b", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 一笔注定失败的转让（同组），不得留下任何轨迹痕迹。
	if _, err := s.Transfer("b", "b", 1, tr.Revision); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("invalid transfer: %v", err)
	}
	res, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if ap := res.Ticks[0].Applied; len(ap) != 1 ||
		ap[0].Kind != tenantsched.ChangeTransfer || ap[0].Revision != tr.Revision ||
		ap[0].From != "a" || ap[0].Group != "b" || ap[0].Amount != 1 {
		t.Fatalf("applied = %+v, want single TRANSFER @%d", ap, tr.Revision)
	}
}
