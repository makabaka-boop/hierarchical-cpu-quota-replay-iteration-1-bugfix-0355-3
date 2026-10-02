package tenantsched_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"tenantsched"
)

// 本文件覆盖 README“可追溯配额借用”一节的约定：
//   - TransferLoan 以唯一编号建立本周期同父组借用，ReturnLoan 只归还该笔
//     尚未使用的余额；
//   - 受让组先消耗自有（非借用）额度，再按借用提交顺序消耗借入额度；
//     子组执行沿路径计入受让组的消耗；
//   - 已被消耗的借用不可归还，其他借用的额度不能顶替；
//   - 周期结束旧借用终结，不得从新周期基础额度归还；
//   - LoanReceipts 与快照、Applied（携带 LoanID）和超期证据一致，迁组
//     前后都能凭逐 tick 轨迹核对额度在哪个组、哪个周期被消耗。

// loanGroups 是借用测试的标准树：a/b/c 为 R 下的兄弟，d 是 b 的子组。
func loanGroups() []tenantsched.GroupSpec {
	return []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 1},
		{ID: "c", Parent: "R", Quota: 3},
		{ID: "d", Parent: "b", Quota: 10},
	}
}

func mustNewLoanScheduler(t *testing.T, groups []tenantsched.GroupSpec) *tenantsched.Scheduler {
	t.Helper()
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func receiptByID(t *testing.T, s *tenantsched.Scheduler, id string) tenantsched.LoanReceipt {
	t.Helper()
	for _, r := range s.LoanReceipts() {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("loan %q not found in receipts %+v", id, s.LoanReceipts())
	return tenantsched.LoanReceipt{}
}

// auditLoanReplay 把产品轨迹当作事件流独立重放借用账：从 Applied 中携带
// LoanID 的 TRANSFER 变更重建每笔借用的建立与归还，按“先消耗自有（非
// 借用）有效额度、再按借用提交顺序消耗”的规则，重新归属每个 RAN tick 沿
// 路径的扣减（含迁组后的新路径），最后与 LoanReceipts 逐字段核对，保证
// 借用明细、各级已用配额、归还变更与逐 tick 轨迹口径一致。
func auditLoanReplay(t *testing.T, s *tenantsched.Scheduler, groups []tenantsched.GroupSpec) {
	t.Helper()
	trace := s.Trace()
	receipts := s.LoanReceipts()

	base := map[string]int{}
	for _, g := range groups {
		base[g.ID] = g.Quota
	}

	type loanAcct struct {
		donor, receiver    string
		amount             int
		returned, consumed int
		period             uint64
		lastRevision       uint64
	}
	accts := map[string]*loanAcct{}
	var grantOrder []string
	active := map[string][]string{} // 受让组 -> 本周期有效借用（建立顺序）
	used := map[string]int{}
	adjust := map[string]int{}

	for _, e := range trace {
		if e.Tick%tenantsched.PeriodTicks == 0 {
			period := e.Tick / tenantsched.PeriodTicks
			for k := range used {
				used[k] = 0
			}
			for k := range adjust {
				adjust[k] = 0
			}
			for recv, ids := range active {
				kept := ids[:0]
				for _, id := range ids {
					if accts[id].period >= period {
						kept = append(kept, id)
					}
				}
				active[recv] = kept
			}
		}
		for _, a := range e.Applied {
			if a.Kind != tenantsched.ChangeTransfer {
				continue
			}
			adjust[a.From] -= a.Amount
			adjust[a.Group] += a.Amount
			if a.LoanID == "" {
				continue
			}
			l, ok := accts[a.LoanID]
			if !ok {
				// 建立借用：From=捐出组、Group=受让组。
				l = &loanAcct{donor: a.From, receiver: a.Group, amount: a.Amount,
					period: e.Tick / tenantsched.PeriodTicks, lastRevision: a.Revision}
				accts[a.LoanID] = l
				grantOrder = append(grantOrder, a.LoanID)
				active[a.Group] = append(active[a.Group], a.LoanID)
			} else {
				// 归还：From=受让组、Group=捐出组。
				if a.From != l.receiver || a.Group != l.donor {
					t.Fatalf("tick %d loan %s return %s->%s, want %s->%s",
						e.Tick, a.LoanID, a.From, a.Group, l.receiver, l.donor)
				}
				l.returned += a.Amount
				l.lastRevision = a.Revision
			}
		}
		if e.Kind != tenantsched.TickRan {
			continue
		}
		for _, gid := range e.Path {
			used[gid]++
			ids := active[gid]
			if len(ids) == 0 {
				continue
			}
			borrowed, consumedSum := 0, 0
			for _, id := range ids {
				l := accts[id]
				borrowed += l.amount - l.returned
				consumedSum += l.consumed
			}
			if used[gid]-consumedSum <= base[gid]+adjust[gid]-borrowed {
				continue // 记在本组自有（非借用）额度
			}
			attributed := false
			for _, id := range ids {
				if l := accts[id]; l.consumed < l.amount-l.returned {
					l.consumed++
					attributed = true
					break
				}
			}
			if !attributed {
				t.Fatalf("tick %d group %s: deduction beyond own+borrowed quota", e.Tick, gid)
			}
		}
	}

	// 回执的数量、顺序（按建立顺序）与逐字段内容必须与重放一致。
	if len(receipts) != len(grantOrder) {
		t.Fatalf("receipts count %d, want %d (grants in trace)", len(receipts), len(grantOrder))
	}
	for i, id := range grantOrder {
		r := receipts[i]
		if r.ID != id {
			t.Fatalf("receipts[%d].ID = %q, want grant order %q", i, r.ID, id)
		}
		l := accts[id]
		if r.Donor != l.donor || r.Receiver != l.receiver || r.Amount != l.amount {
			t.Fatalf("loan %s receipt %+v, replay grant %+v", id, r, l)
		}
		if r.Returned != l.returned || r.Consumed != l.consumed {
			t.Fatalf("loan %s receipt returned/consumed %d/%d, replay %d/%d",
				id, r.Returned, r.Consumed, l.returned, l.consumed)
		}
		if r.Period != l.period || r.Revision != l.lastRevision {
			t.Fatalf("loan %s receipt period/revision %d/%d, replay %d/%d",
				id, r.Period, r.Revision, l.period, l.lastRevision)
		}
	}
}

// TestLoanConsumptionOrderAndReturn 多笔借用指向同一受让组：先消耗基础
// 额度，再按借用提交顺序消耗；已耗尽的借用显示已用且不可归还，不能用
// 另一笔未用借用的额度顶替归还。
func TestLoanConsumptionOrderAndReturn(t *testing.T) {
	s := mustNewLoanScheduler(t, loanGroups())

	if _, err := s.Submit(tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 3, Deadline: 9, Group: "b"}, 0); err != nil {
		t.Fatal(err)
	}
	l1, err := s.TransferLoan("L1", "a", "b", 2, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	l2, err := s.TransferLoan("L2", "c", "b", 2, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if l1.Period != 0 || l2.Period != 0 || l1.Consumed != 0 || l2.Consumed != 0 {
		t.Fatalf("fresh receipts = %+v / %+v", l1, l2)
	}

	res, err := s.Advance(3, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// tick0 开头按提交修订号顺序入账三笔变更，且借用 TRANSFER 携带编号。
	ap := res.Ticks[0].Applied
	if len(ap) != 3 || ap[0].Kind != tenantsched.ChangeSubmit ||
		ap[1].Kind != tenantsched.ChangeTransfer || ap[1].LoanID != "L1" ||
		ap[1].From != "a" || ap[1].Group != "b" || ap[1].Amount != 2 ||
		ap[2].Kind != tenantsched.ChangeTransfer || ap[2].LoanID != "L2" ||
		ap[2].From != "c" || ap[2].Group != "b" || ap[2].Amount != 2 {
		t.Fatalf("tick0 applied = %+v", ap)
	}
	// tick0..2 执行 j：b 已用 1（自有）、2（L1）、3（L1）。
	for i, e := range res.Ticks {
		if e.Kind != tenantsched.TickRan || e.JobID != "j" || e.Deducted[0] != i+1 {
			t.Fatalf("tick %d = %+v, want RAN j with b used %d", e.Tick, e, i+1)
		}
	}

	// L1 按约定顺序已被消耗 2，必须显示已用；L2 尚未被动用。
	r1 := receiptByID(t, s, "L1")
	if r1.Consumed != 2 || r1.Returned != 0 || r1.Amount != 2 {
		t.Fatalf("L1 receipt = %+v, want consumed 2/returned 0/amount 2", r1)
	}
	r2 := receiptByID(t, s, "L2")
	if r2.Consumed != 0 || r2.Returned != 0 {
		t.Fatalf("L2 receipt = %+v, want untouched", r2)
	}

	// 已耗尽的 L1 不可归还：即使受让组凭 L2 仍有未用额度，也不能顶替。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return consumed L1: %v", err)
	}
	// 超出本笔未用余额的归还同样被拒。
	if _, err := s.ReturnLoan("L2", 3, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond unused: %v", err)
	}
	// L2 未使用，可以全额归还。
	rt, err := s.ReturnLoan("L2", 2, s.Revision())
	if err != nil {
		t.Fatalf("return unused L2: %v", err)
	}
	if rt.Returned != 2 || rt.Consumed != 0 {
		t.Fatalf("L2 after return = %+v", rt)
	}

	res2, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// 归还作为携带 LoanID 的 TRANSFER 入账（方向：受让组 -> 捐出组）。
	ap3 := res2.Ticks[0].Applied
	if len(ap3) != 1 || ap3[0].Kind != tenantsched.ChangeTransfer || ap3[0].LoanID != "L2" ||
		ap3[0].From != "b" || ap3[0].Group != "c" || ap3[0].Amount != 2 || ap3[0].Revision != rt.Revision {
		t.Fatalf("tick3 applied = %+v, want return of L2", ap3)
	}
	if !strings.Contains(tenantsched.RenderTrace(res2.Ticks), "loan=L2") {
		t.Fatalf("rendered trace should carry loan id:\n%s", tenantsched.RenderTrace(res2.Ticks))
	}

	// 快照三视图与回执一致：b 有效额度 = 基础 1 + L1 未归还 2。
	snap := s.Snapshot()
	if qb := quotaByID(snap.Quotas, "b"); qb.Quota != 1 || qb.Effective != 3 || qb.Used != 3 {
		t.Fatalf("b quota view = %+v, want 1/3/3", qb)
	}
	if qc := quotaByID(snap.Quotas, "c"); qc.Effective != 3 || qc.Used != 0 {
		t.Fatalf("c quota view = %+v, want effective 3 after return", qc)
	}
	if qa := quotaByID(snap.Quotas, "a"); qa.Effective != 0 {
		t.Fatalf("a quota view = %+v, want effective 0 (lent out)", qa)
	}
	auditLoanReplay(t, s, loanGroups())
}

// TestLoanSubgroupConsumptionCountsTowardReceiver 受让组的子组执行时，
// 沿路径扣减受让组配额，同样按规则计入受让组借用的消耗。
func TestLoanSubgroupConsumptionCountsTowardReceiver(t *testing.T) {
	s := mustNewLoanScheduler(t, loanGroups())

	if _, err := s.TransferLoan("L1", "a", "b", 2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 3, Deadline: 9, Group: "d"}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(3, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// 每个 tick 沿 d>b>R 扣减；b 的 3 次扣减依次是自有 1、L1 两笔。
	for i, e := range res.Ticks {
		if e.Kind != tenantsched.TickRan || e.Group != "d" || len(e.Path) != 3 || e.Path[1] != "b" {
			t.Fatalf("tick %d = %+v, want RAN along d>b>R", e.Tick, e)
		}
		if e.Deducted[1] != i+1 {
			t.Fatalf("tick %d b deducted = %d, want %d", e.Tick, e.Deducted[1], i+1)
		}
	}
	r := receiptByID(t, s, "L1")
	if r.Consumed != 2 {
		t.Fatalf("L1 consumed = %d, want 2 (subgroup execution counts toward receiver)", r.Consumed)
	}
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return consumed loan: %v", err)
	}
	auditLoanReplay(t, s, loanGroups())
}

// TestLoanExpiresAtPeriodBoundary 周期结束旧借用终结：不得从新周期基础
// 额度归还旧借用；边界临界提交的借用属于新周期，重置后仍可归属消耗。
func TestLoanExpiresAtPeriodBoundary(t *testing.T) {
	s := mustNewLoanScheduler(t, []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 3},
		{ID: "b", Parent: "R", Quota: 1},
	})

	if _, err := s.TransferLoan("L1", "a", "b", 2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j1", ReleaseAt: 0, Work: 1, Deadline: 9, Group: "b"}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick0：j1 用掉 b 的自有额度
		t.Fatal(err)
	}
	if _, err := s.Advance(9, s.Revision()); err != nil { // tick1..9 空转，now=10
		t.Fatal(err)
	}

	// 跨入新周期：旧借用 L1 已终结，不能拿 b 新周期的基础额度归还。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return expired loan: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 2, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return expired loan (full): %v", err)
	}
	// 旧借用回执保留为审计记录：未消耗、未归还、周期 0。
	r1 := receiptByID(t, s, "L1")
	if r1.Consumed != 0 || r1.Returned != 0 || r1.Period != 0 {
		t.Fatalf("L1 receipt = %+v, want untouched period-0 record", r1)
	}
	// 新周期快照：两组恢复基础额度，旧借用不留痕迹。
	snap := s.Snapshot()
	if qb := quotaByID(snap.Quotas, "b"); qb.Effective != 1 || qb.Used != 0 {
		t.Fatalf("b quota view = %+v, want effective 1/used 0 in new period", qb)
	}
	if qa := quotaByID(snap.Quotas, "a"); qa.Effective != 3 || qa.Used != 0 {
		t.Fatalf("a quota view = %+v, want effective 3/used 0 in new period", qa)
	}

	// 边界临界提交（now=10，下一 tick 即边界）的借用属于新周期。
	l2, err := s.TransferLoan("L2", "a", "b", 2, s.Revision())
	if err != nil {
		t.Fatalf("boundary loan: %v", err)
	}
	if l2.Period != 1 {
		t.Fatalf("L2 period = %d, want 1 (boundary commit belongs to new period)", l2.Period)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j2", ReleaseAt: 10, Work: 3, Deadline: 19, Group: "b"}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(3, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// tick10：周期重置终结 L1、保留 L2，随后入账 L2 与 j2 并执行。
	ap10 := res.Ticks[0].Applied
	if len(ap10) != 2 || ap10[0].Kind != tenantsched.ChangeTransfer || ap10[0].LoanID != "L2" ||
		ap10[1].Kind != tenantsched.ChangeSubmit {
		t.Fatalf("tick10 applied = %+v, want L2 grant then submit", ap10)
	}
	for i, e := range res.Ticks {
		if e.Kind != tenantsched.TickRan || e.JobID != "j2" {
			t.Fatalf("tick %d = %+v, want RAN j2", e.Tick, e)
		}
		if e.Period != 1 {
			t.Fatalf("tick %d period = %d, want 1", e.Tick, e.Period)
		}
		_ = i
	}
	// j2 的 3 次执行：自有 1 + L1 已终结不得再用，消耗的是新周期的 L2。
	r2 := receiptByID(t, s, "L2")
	if r2.Consumed != 2 || r2.Period != 1 {
		t.Fatalf("L2 receipt = %+v, want consumed 2 in period 1", r2)
	}
	if r1 = receiptByID(t, s, "L1"); r1.Consumed != 0 || r1.Returned != 0 {
		t.Fatalf("expired L1 changed after boundary: %+v", r1)
	}
	if _, err := s.ReturnLoan("L2", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return fully consumed L2: %v", err)
	}
	auditLoanReplay(t, s, []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 3},
		{ID: "b", Parent: "R", Quota: 1},
	})
}

// TestLoanMigrationReconciliation 作业在执行期间迁组：借用消耗必须记在
// 执行时所在组（路径祖先）的账上，回执、各级已用配额、归还变更与逐 tick
// 轨迹可互相核对；跨周期后旧借用终结。
func TestLoanMigrationReconciliation(t *testing.T) {
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 4},
		{ID: "b", Parent: "R", Quota: 2},
		{ID: "x", Parent: "R", Quota: 4},
		{ID: "y", Parent: "R", Quota: 1},
	}
	s := mustNewLoanScheduler(t, groups)

	if _, err := s.Submit(tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 6, Deadline: 30, Group: "b"}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L1", "a", "b", 3, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L2", "x", "y", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	// tick0..3：j 在 b 执行 4 次——自有 2 + L1 两笔。
	if _, err := s.Advance(4, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if r := receiptByID(t, s, "L1"); r.Consumed != 2 {
		t.Fatalf("L1 consumed = %d, want 2 before migration", r.Consumed)
	}
	// 迁组不退还已消耗：L1 账保持不变；j 在 y 继续执行，消耗 y 的借用。
	if _, err := s.Migrate("j", "y", s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(2, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if e := res.Ticks[0]; e.Kind != tenantsched.TickRan || e.Group != "y" || len(e.Path) != 2 || e.Path[0] != "y" {
		t.Fatalf("tick4 = %+v, want RAN j along y>R", e)
	}
	if j := s.Snapshot().Jobs[0]; j.State != tenantsched.JobCompleted || j.Group != "y" {
		t.Fatalf("job = %+v, want completed in y", j)
	}
	if r := receiptByID(t, s, "L1"); r.Consumed != 2 || r.Returned != 0 {
		t.Fatalf("L1 changed by migration-period execution: %+v", r)
	}
	if r := receiptByID(t, s, "L2"); r.Consumed != 1 {
		t.Fatalf("L2 consumed = %d, want 1 (execution after migration)", r.Consumed)
	}

	// 部分归还：L1 已消耗 2，只能归还剩余的 1。
	if _, err := s.ReturnLoan("L1", 2, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond L1 unused: %v", err)
	}
	rt1, err := s.ReturnLoan("L1", 1, s.Revision())
	if err != nil {
		t.Fatalf("partial return L1: %v", err)
	}
	if rt1.Returned != 1 || rt1.Consumed != 2 {
		t.Fatalf("L1 after partial return = %+v", rt1)
	}
	res2, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if ap := res2.Ticks[0].Applied; len(ap) != 1 || ap[0].LoanID != "L1" ||
		ap[0].From != "b" || ap[0].Group != "a" || ap[0].Amount != 1 {
		t.Fatalf("tick6 applied = %+v, want return of L1", ap)
	}
	if _, err := s.ReturnLoan("L2", 1, s.Revision()); err != nil {
		t.Fatalf("partial return L2: %v", err)
	}
	if _, err := s.Advance(3, s.Revision()); err != nil { // tick7..9，now=10
		t.Fatal(err)
	}

	// 跨周期：两笔借用都终结，归还一律被拒。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return L1 across period: %v", err)
	}
	if _, err := s.ReturnLoan("L2", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return L2 across period: %v", err)
	}
	// 回执保留最终账：L1 消耗 2 归还 1，L2 消耗 1 归还 1。
	r1, r2 := receiptByID(t, s, "L1"), receiptByID(t, s, "L2")
	if r1.Consumed != 2 || r1.Returned != 1 || r1.Period != 0 {
		t.Fatalf("L1 final = %+v, want consumed 2 returned 1 period 0", r1)
	}
	if r2.Consumed != 1 || r2.Returned != 1 || r2.Period != 0 {
		t.Fatalf("L2 final = %+v, want consumed 1 returned 1 period 0", r2)
	}
	// 新周期快照恢复基础额度。
	snap := s.Snapshot()
	if qb := quotaByID(snap.Quotas, "b"); qb.Effective != 2 || qb.Used != 0 {
		t.Fatalf("b quota view = %+v, want reset to base", qb)
	}
	if qy := quotaByID(snap.Quotas, "y"); qy.Effective != 1 || qy.Used != 0 {
		t.Fatalf("y quota view = %+v, want reset to base", qy)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick10：周期重置
		t.Fatal(err)
	}
	auditLoanReplay(t, s, groups)
}

// TestLoanValidation 覆盖借用接口的参数校验、校验顺序与修订号冲突语义。
func TestLoanValidation(t *testing.T) {
	s := mustNewLoanScheduler(t, loanGroups())

	cases := []struct {
		name string
		run  func() error
		want error
	}{
		{"empty id", func() error { _, err := s.TransferLoan("", "a", "b", 1, 0); return err }, tenantsched.ErrInvalidArgument},
		{"unknown donor", func() error { _, err := s.TransferLoan("L1", "ghost", "b", 1, 0); return err }, tenantsched.ErrNotFound},
		{"unknown receiver", func() error { _, err := s.TransferLoan("L1", "a", "ghost", 1, 0); return err }, tenantsched.ErrNotFound},
		{"same group", func() error { _, err := s.TransferLoan("L1", "a", "a", 1, 0); return err }, tenantsched.ErrInvalidArgument},
		{"root donor", func() error { _, err := s.TransferLoan("L1", "R", "b", 1, 0); return err }, tenantsched.ErrInvalidArgument},
		{"not siblings", func() error { _, err := s.TransferLoan("L1", "a", "d", 1, 0); return err }, tenantsched.ErrInvalidArgument},
		{"zero amount", func() error { _, err := s.TransferLoan("L1", "a", "b", 0, 0); return err }, tenantsched.ErrInvalidArgument},
		{"negative amount", func() error { _, err := s.TransferLoan("L1", "a", "b", -1, 0); return err }, tenantsched.ErrInvalidArgument},
		{"exceeds unused", func() error { _, err := s.TransferLoan("L1", "a", "b", 3, 0); return err }, tenantsched.ErrInvalidArgument},
		{"stale revision", func() error { _, err := s.TransferLoan("L1", "a", "b", 1, 99); return err }, tenantsched.ErrConflict},
	}
	for _, tc := range cases {
		if err := tc.run(); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := len(s.LoanReceipts()); n != 0 {
		t.Fatalf("failed grants left %d receipts, want 0", n)
	}

	l, err := s.TransferLoan("L1", "a", "b", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if l.Revision != 1 || l.Amount != 2 || l.Donor != "a" || l.Receiver != "b" {
		t.Fatalf("grant receipt = %+v", l)
	}
	// 编号唯一：重复编号先于修订号比较被拒绝。
	if _, err := s.TransferLoan("L1", "c", "b", 1, 99); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("duplicate id: %v", err)
	}
	// 归还校验顺序：存在性 -> 参数 -> 状态（未用余额）-> 修订号。
	if _, err := s.ReturnLoan("ghost", 1, 99); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("return unknown loan: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 0, 99); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return zero: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 3, 99); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond unused: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 1, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("return with stale revision: %v", err)
	}
	// 分两次全额归还；第三次因未用余额为 0 被拒。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); err != nil {
		t.Fatalf("first partial return: %v", err)
	}
	rt, err := s.ReturnLoan("L1", 1, s.Revision())
	if err != nil {
		t.Fatalf("second partial return: %v", err)
	}
	if rt.Returned != 2 || rt.Consumed != 0 {
		t.Fatalf("receipt after two returns = %+v", rt)
	}
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond amount: %v", err)
	}
	// 建立与两笔归还在同一 tick 入账，净额归零。
	res, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	ap := res.Ticks[0].Applied
	if len(ap) != 3 || ap[0].LoanID != "L1" || ap[1].LoanID != "L1" || ap[2].LoanID != "L1" ||
		ap[0].From != "a" || ap[1].From != "b" || ap[2].From != "b" {
		t.Fatalf("tick0 applied = %+v, want grant then two returns of L1", ap)
	}
	snap := s.Snapshot()
	if qa := quotaByID(snap.Quotas, "a"); qa.Effective != 2 {
		t.Fatalf("a effective = %d, want 2 (grant and returns net to zero)", qa.Effective)
	}
	if qb := quotaByID(snap.Quotas, "b"); qb.Effective != 1 {
		t.Fatalf("b effective = %d, want 1", qb.Effective)
	}
	auditLoanReplay(t, s, loanGroups())
}

// TestLoanPlainTransferConsumedBeforeLoan 自有（非借用）有效额度——含普通
// 转让净额——先于借用被消耗，借用始终最后被消耗。
func TestLoanPlainTransferConsumedBeforeLoan(t *testing.T) {
	s := mustNewLoanScheduler(t, loanGroups())

	if _, err := s.Transfer("a", "b", 1, 0); err != nil { // 普通转让：b 自有有效额度 +1
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L1", "c", "b", 1, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 3, Deadline: 9, Group: "b"}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(3, s.Revision()); err != nil {
		t.Fatal(err)
	}
	// b 有效额度 3 = 基础 1 + 普通转让 1 + 借用 1：前两次消耗记自有，
	// 第三次才消耗 L1。
	r := receiptByID(t, s, "L1")
	if r.Consumed != 1 {
		t.Fatalf("L1 consumed = %d, want 1 (own quota incl. plain transfer first)", r.Consumed)
	}
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return consumed loan: %v", err)
	}
	auditLoanReplay(t, s, loanGroups())
}

// TestLoanOverdueEvidenceConsistency 借用额度耗尽后作业被阻挡并超期：
// 超期证据的阻挡祖先按含借用的有效额度给出，回执显示借用已耗尽，口径一致。
func TestLoanOverdueEvidenceConsistency(t *testing.T) {
	s := mustNewLoanScheduler(t, []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 1},
	})

	if _, err := s.TransferLoan("L1", "a", "b", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{ID: "j", ReleaseAt: 0, Work: 5, Deadline: 4, Group: "b"}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(5, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	// tick0/1 执行（自有 1 + L1 一笔）；tick2..4 被 b（含借用有效额度 2）阻挡。
	for i, e := range res.Ticks {
		switch {
		case i < 2:
			if e.Kind != tenantsched.TickRan || e.JobID != "j" {
				t.Fatalf("tick %d = %+v, want RAN j", e.Tick, e)
			}
		default:
			if e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "b" {
				t.Fatalf("tick %d = %+v, want IDLE_BLOCKED by b", e.Tick, e)
			}
		}
	}
	if len(res.Overdue) != 1 {
		t.Fatalf("overdue = %+v, want exactly 1 evidence", res.Overdue)
	}
	ev := res.Overdue[0]
	if ev.JobID != "j" || ev.Remaining != 3 || ev.Blocker != "b" || ev.Group != "b" {
		t.Fatalf("overdue evidence = %+v, want remaining 3 blocker b", ev)
	}
	// 回执与超期证据一致：L1 已随 tick1 的执行耗尽，不可归还。
	r := receiptByID(t, s, "L1")
	if r.Consumed != 1 || r.Returned != 0 {
		t.Fatalf("L1 receipt = %+v, want consumed 1", r)
	}
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return consumed loan: %v", err)
	}
	auditLoanReplay(t, s, []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 2},
		{ID: "b", Parent: "R", Quota: 1},
	})
}

// TestLoanConcurrentBarrier 屏障下并发建立/归还借用：持同一修订号时最多
// 一笔成功，失败者不留任何痕迹。
func TestLoanConcurrentBarrier(t *testing.T) {
	s := mustNewLoanScheduler(t, []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 1000},
		{ID: "a", Parent: "R", Quota: 100},
		{ID: "b", Parent: "R", Quota: 1},
	})

	const contenders = 16
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(contenders)

	var wins, conflicts int64
	for i := 0; i < contenders; i++ {
		i := i
		go func() {
			defer done.Done()
			start.Wait()
			_, err := s.TransferLoan(fmt.Sprintf("L%02d", i), "a", "b", 1, 0)
			switch {
			case err == nil:
				atomic.AddInt64(&wins, 1)
			case errors.Is(err, tenantsched.ErrConflict):
				atomic.AddInt64(&conflicts, 1)
			default:
				t.Errorf("unexpected grant error: %v", err)
			}
		}()
	}
	start.Done()
	done.Wait()
	if wins != 1 || conflicts != contenders-1 {
		t.Fatalf("grant race: wins=%d conflicts=%d, want 1/%d", wins, conflicts, contenders-1)
	}
	receipts := s.LoanReceipts()
	if len(receipts) != 1 {
		t.Fatalf("receipts after grant race = %+v, want exactly 1", receipts)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // 入账借用
		t.Fatal(err)
	}

	// 并发归还同一笔借用（全额 1）：同样最多一笔成功。失败者要么撞修订号
	// 冲突，要么因赢家已全额归还而撞“未用余额为 0”——两者都是无副作用
	// 的拒绝（参数/状态校验先于修订号比较）。
	id := receipts[0].ID
	rev := s.Revision()
	var start2 sync.WaitGroup
	start2.Add(1)
	var done2 sync.WaitGroup
	done2.Add(contenders)
	var rwins, rrejected int64
	for i := 0; i < contenders; i++ {
		go func() {
			defer done2.Done()
			start2.Wait()
			_, err := s.ReturnLoan(id, 1, rev)
			switch {
			case err == nil:
				atomic.AddInt64(&rwins, 1)
			case errors.Is(err, tenantsched.ErrConflict) || errors.Is(err, tenantsched.ErrInvalidArgument):
				atomic.AddInt64(&rrejected, 1)
			default:
				t.Errorf("unexpected return error: %v", err)
			}
		}()
	}
	start2.Done()
	done2.Wait()
	if rwins != 1 || rrejected != contenders-1 {
		t.Fatalf("return race: wins=%d rejected=%d, want 1/%d", rwins, rrejected, contenders-1)
	}
	r := receiptByID(t, s, id)
	if r.Returned != 1 || r.Consumed != 0 {
		t.Fatalf("receipt after return race = %+v, want returned 1", r)
	}
	// 已全额归还：任何进一步的归还都必须因未用余额为 0 被拒。
	if _, err := s.ReturnLoan(id, 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return after full return: %v", err)
	}
}
