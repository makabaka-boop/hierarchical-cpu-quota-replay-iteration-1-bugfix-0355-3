package tenantsched_test

import (
	"errors"
	"testing"

	"tenantsched"
)

// loanGroups 是借用测试的标准树：d1/d2 为捐出组，g 为受让组，三者同父。
func loanGroups() []tenantsched.GroupSpec {
	return []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d1", Parent: "R", Quota: 2},
		{ID: "d2", Parent: "R", Quota: 2},
		{ID: "g", Parent: "R", Quota: 1},
	}
}

// replayLoans 把产品轨迹当事件流独立重放借用账目：周期边界清零已用量，
// Applied 中的 TRANSFER 按方向匹配借用的归还（受让组 -> 捐出组），RAN
// tick 沿路径扣减并按“先基础额度、再按借用提交顺序”归属到各笔借用；
// 最终重放结果必须与 LoanReceipts 的 Consumed/Returned 完全一致，
// 以此核对“额度究竟在哪个组、哪个周期被消耗”。
func replayLoans(t *testing.T, trace []tenantsched.TraceEntry, groups []tenantsched.GroupSpec, receipts []tenantsched.LoanReceipt) {
	t.Helper()
	base := map[string]int{}
	for _, g := range groups {
		base[g.ID] = g.Quota
	}
	byReceiver := map[string][]tenantsched.LoanReceipt{}
	for _, l := range receipts {
		byReceiver[l.Receiver] = append(byReceiver[l.Receiver], l)
	}
	used := map[string]int{}
	returned := map[string]int{}
	consumed := map[string]int{}
	for _, e := range trace {
		if e.Tick%tenantsched.PeriodTicks == 0 {
			used = map[string]int{}
		}
		for _, a := range e.Applied {
			if a.Kind != tenantsched.ChangeTransfer {
				continue
			}
			// 归还（受让组 -> 捐出组）按方向与周期归属到对应借用。
			period := e.Tick / tenantsched.PeriodTicks
			for _, l := range receipts {
				if a.From == l.Receiver && a.Group == l.Donor && l.Period == period &&
					returned[l.ID] < l.Amount {
					returned[l.ID] += a.Amount
					break
				}
			}
		}
		if e.Kind != tenantsched.TickRan {
			continue
		}
		period := e.Tick / tenantsched.PeriodTicks
		for _, gid := range e.Path {
			used[gid]++
			if used[gid] <= base[gid] {
				continue
			}
			for _, l := range byReceiver[gid] {
				if l.Period != period {
					continue
				}
				if consumed[l.ID] < l.Amount-returned[l.ID] {
					consumed[l.ID]++
					break
				}
			}
		}
	}
	for _, l := range receipts {
		if consumed[l.ID] != l.Consumed {
			t.Fatalf("loan %s replay consumed %d, receipt says %d", l.ID, consumed[l.ID], l.Consumed)
		}
		if returned[l.ID] != l.Returned {
			t.Fatalf("loan %s replay returned %d, receipt says %d", l.ID, returned[l.ID], l.Returned)
		}
	}
}

// TestLoanConsumptionOrderAndReturn 多笔借用借给同一受让组：执行先消耗基础
// 额度，再按借用提交顺序消耗借入额度；已被消耗的借用余额不允许归还，也
// 不能用另一笔尚未使用的借用代替归还。
func TestLoanConsumptionOrderAndReturn(t *testing.T) {
	groups := loanGroups()
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jg", ReleaseAt: 0, Work: 4, Deadline: 9, Group: "g",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick0：g 基础额度 1 用完
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L1", "d1", "g", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L2", "d2", "g", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(3, s.Revision()) // tick1..3：借入额度按 L1 -> L2 顺序消耗
	if err != nil {
		t.Fatal(err)
	}
	// tick1 开头按提交顺序入账两笔借用。
	if ap := res.Ticks[0].Applied; len(ap) != 2 ||
		ap[0].Kind != tenantsched.ChangeTransfer || ap[0].From != "d1" || ap[0].Group != "g" || ap[0].Amount != 2 ||
		ap[1].Kind != tenantsched.ChangeTransfer || ap[1].From != "d2" || ap[1].Group != "g" || ap[1].Amount != 2 {
		t.Fatalf("tick1 applied = %+v, want TRANSFER d1->g x2 then d2->g x2", ap)
	}

	receipts := s.LoanReceipts()
	if len(receipts) != 2 || receipts[0].ID != "L1" || receipts[1].ID != "L2" {
		t.Fatalf("receipts = %+v, want submission order L1,L2", receipts)
	}
	// g 已用 4：基础 1 + L1 2 + L2 1（按提交顺序归属）。
	if l1 := receipts[0]; l1.Amount != 2 || l1.Consumed != 2 || l1.Returned != 0 {
		t.Fatalf("L1 = %+v, want amount 2 consumed 2 returned 0", l1)
	}
	if l2 := receipts[1]; l2.Amount != 2 || l2.Consumed != 1 || l2.Returned != 0 {
		t.Fatalf("L2 = %+v, want amount 2 consumed 1 returned 0", l2)
	}

	// L1 已被全部消耗：任何归还都必须被拒（不能用 L2 的余额代替）。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return fully consumed L1: %v", err)
	}
	// L2 只剩 1 未用：归还 2 越界。
	if _, err := s.ReturnLoan("L2", 2, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond unused: %v", err)
	}
	// 归还 L2 未用的 1：合法，作为 TRANSFER g->d2 在下一 tick 入账。
	if _, err := s.ReturnLoan("L2", 1, s.Revision()); err != nil {
		t.Fatalf("return unused portion: %v", err)
	}
	res, err = s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if ap := res.Ticks[0].Applied; len(ap) != 1 || ap[0].Kind != tenantsched.ChangeTransfer ||
		ap[0].From != "g" || ap[0].Group != "d2" || ap[0].Amount != 1 {
		t.Fatalf("return applied = %+v, want TRANSFER g->d2 x1", ap)
	}
	if l2 := s.LoanReceipts()[1]; l2.Consumed != 1 || l2.Returned != 1 {
		t.Fatalf("L2 after return = %+v, want consumed 1 returned 1", l2)
	}

	// 归还只动两组有效额度：祖先 R 与历史扣减不变。
	snap := s.Snapshot()
	if qg := quotaByID(snap.Quotas, "g"); qg.Quota != 1 || qg.Effective != 4 || qg.Used != 4 {
		t.Fatalf("g = %+v, want base 1 effective 4 used 4", qg)
	}
	if qd1 := quotaByID(snap.Quotas, "d1"); qd1.Effective != 0 || qd1.Used != 0 {
		t.Fatalf("d1 = %+v, want effective 0 used 0", qd1)
	}
	if qd2 := quotaByID(snap.Quotas, "d2"); qd2.Effective != 1 || qd2.Used != 0 {
		t.Fatalf("d2 = %+v, want effective 1 used 0", qd2)
	}
	if qr := quotaByID(snap.Quotas, "R"); qr.Effective != 100 || qr.Used != 4 {
		t.Fatalf("R = %+v, want effective 100 used 4 (ancestor untouched)", qr)
	}
	replayLoans(t, s.Trace(), groups, s.LoanReceipts())
}

// TestLoanSubgroupExecutionConsumesAncestorLoan 子组执行沿路径扣减祖先配额，
// 祖先作为受让组的借用同样按顺序被消耗并计入台账。
func TestLoanSubgroupExecutionConsumesAncestorLoan(t *testing.T) {
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 3},
		{ID: "p", Parent: "R", Quota: 1},
		{ID: "c", Parent: "p", Quota: 10},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L1", "d", "p", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jc", ReleaseAt: 0, Work: 3, Deadline: 9, Group: "c",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(3, s.Revision()); err != nil { // tick0..2：jc 沿 c>p>R 执行
		t.Fatal(err)
	}
	receipts := s.LoanReceipts()
	if len(receipts) != 1 || receipts[0].Consumed != 2 {
		t.Fatalf("receipts = %+v, want L1 consumed 2 (p used 3 = base 1 + loan 2)", receipts)
	}
	// 借入份额已被子组执行耗尽：不允许归还。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return loan consumed via subgroup: %v", err)
	}
	replayLoans(t, s.Trace(), groups, s.LoanReceipts())
}

// TestLoanExpiresAtPeriodBoundary 周期结束旧借用终结：跨入新周期后归还旧
// 借用必须被拒，且不得动用新周期基础额度；台账仍保留历史借用供核对。
func TestLoanExpiresAtPeriodBoundary(t *testing.T) {
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 3},
		{ID: "g", Parent: "R", Quota: 2},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	l1, err := s.TransferLoan("L1", "d", "g", 2, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if l1.Period != 0 {
		t.Fatalf("L1 period = %d, want 0", l1.Period)
	}
	if _, err := s.Advance(10, s.Revision()); err != nil { // tick0..9，进入周期 1
		t.Fatal(err)
	}
	// 旧借用已终结：归还必须被拒，新周期基础额度分毫不动。
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return terminated loan: %v", err)
	}
	snap := s.Snapshot()
	if qg := quotaByID(snap.Quotas, "g"); qg.Effective != 2 || qg.Used != 0 {
		t.Fatalf("g = %+v, want fresh cycle effective 2 used 0", qg)
	}
	if qd := quotaByID(snap.Quotas, "d"); qd.Effective != 3 || qd.Used != 0 {
		t.Fatalf("d = %+v, want fresh cycle effective 3 used 0", qd)
	}
	// 台账保留已终结的历史借用。
	receipts := s.LoanReceipts()
	if len(receipts) != 1 || receipts[0].ID != "L1" || receipts[0].Period != 0 ||
		receipts[0].Returned != 0 || receipts[0].Consumed != 0 {
		t.Fatalf("receipts = %+v, want terminated L1 of period 0", receipts)
	}
	// 新周期可以正常建立并归还新借用。
	l2, err := s.TransferLoan("L2", "d", "g", 2, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if l2.Period != 1 {
		t.Fatalf("L2 period = %d, want 1", l2.Period)
	}
	if _, err := s.ReturnLoan("L2", 1, s.Revision()); err != nil {
		t.Fatalf("return fresh loan: %v", err)
	}
	res, err := s.Advance(1, s.Revision()) // tick10：入账建立与归还两笔 TRANSFER
	if err != nil {
		t.Fatal(err)
	}
	if ap := res.Ticks[0].Applied; len(ap) != 2 ||
		ap[0].From != "d" || ap[0].Group != "g" || ap[0].Amount != 2 ||
		ap[1].From != "g" || ap[1].Group != "d" || ap[1].Amount != 1 {
		t.Fatalf("tick10 applied = %+v, want TRANSFER d->g x2 then g->d x1", ap)
	}
	snap = s.Snapshot()
	if qg := quotaByID(snap.Quotas, "g"); qg.Effective != 3 {
		t.Fatalf("g = %+v, want effective 3 (2 +2 -1)", qg)
	}
	if qd := quotaByID(snap.Quotas, "d"); qd.Effective != 2 {
		t.Fatalf("d = %+v, want effective 2 (3 -2 +1)", qd)
	}
	if l2 := s.LoanReceipts()[1]; l2.Returned != 1 || l2.Consumed != 0 || l2.Period != 1 {
		t.Fatalf("L2 = %+v, want returned 1 consumed 0 period 1", l2)
	}
	replayLoans(t, s.Trace(), groups, s.LoanReceipts())
}

// TestLoanMigrationAttribution 作业执行期间迁组：已执行的扣减留在旧组账目
// （归属旧组的借用），迁移后的执行归属新组；借用台账、各级已用配额、归还
// 变更与逐 tick 轨迹口径一致。
func TestLoanMigrationAttribution(t *testing.T) {
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d", Parent: "R", Quota: 3},
		{ID: "g1", Parent: "R", Quota: 1},
		{ID: "g2", Parent: "R", Quota: 2},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransferLoan("L1", "d", "g1", 2, s.Revision()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "jm", ReleaseAt: 0, Work: 4, Deadline: 20, Group: "g1",
	}, s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err := s.Advance(2, s.Revision()) // tick0/1：jm 在 g1 执行，g1 已用 2（基础 1 + L1 1）
	if err != nil {
		t.Fatal(err)
	}
	if res.Ticks[0].Group != "g1" || res.Ticks[1].Group != "g1" {
		t.Fatalf("tick0/1 group = %s/%s, want g1/g1", res.Ticks[0].Group, res.Ticks[1].Group)
	}
	if _, err := s.Migrate("jm", "g2", s.Revision()); err != nil {
		t.Fatal(err)
	}
	res, err = s.Advance(2, s.Revision()) // tick2/3：jm 在 g2 执行并完成
	if err != nil {
		t.Fatal(err)
	}
	if res.Ticks[0].Group != "g2" || res.Ticks[1].Group != "g2" {
		t.Fatalf("tick2/3 group = %s/%s, want g2/g2", res.Ticks[0].Group, res.Ticks[1].Group)
	}
	// L1 只被迁移前 g1 的执行消耗 1；迁移不退还历史扣减。
	receipts := s.LoanReceipts()
	if len(receipts) != 1 || receipts[0].Receiver != "g1" || receipts[0].Consumed != 1 || receipts[0].Returned != 0 {
		t.Fatalf("receipts = %+v, want L1 receiver g1 consumed 1 returned 0", receipts)
	}
	// 可归还余额 = 2 − 1 = 1：归还 2 被拒，归还 1 成功。
	if _, err := s.ReturnLoan("L1", 2, s.Revision()); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond consumed-adjusted unused: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 1, s.Revision()); err != nil {
		t.Fatalf("return remaining unused: %v", err)
	}
	if _, err := s.Advance(1, s.Revision()); err != nil { // tick4：入账归还
		t.Fatal(err)
	}
	// 快照口径：g1 历史扣减 2 不退还，g2 记录迁移后的 2。
	snap := s.Snapshot()
	if qg1 := quotaByID(snap.Quotas, "g1"); qg1.Effective != 2 || qg1.Used != 2 {
		t.Fatalf("g1 = %+v, want effective 2 (1 +2 -1) used 2", qg1)
	}
	if qg2 := quotaByID(snap.Quotas, "g2"); qg2.Effective != 2 || qg2.Used != 2 {
		t.Fatalf("g2 = %+v, want effective 2 used 2", qg2)
	}
	if qd := quotaByID(snap.Quotas, "d"); qd.Effective != 2 || qd.Used != 0 {
		t.Fatalf("d = %+v, want effective 2 (3 -2 +1) used 0", qd)
	}
	if l1 := s.LoanReceipts()[0]; l1.Consumed != 1 || l1.Returned != 1 || l1.Period != 0 {
		t.Fatalf("L1 = %+v, want consumed 1 returned 1 period 0", l1)
	}
	replayLoans(t, s.Trace(), groups, s.LoanReceipts())
}

// TestLoanValidationAndRevision 借用接口的参数校验、校验顺序与失败无副作用。
func TestLoanValidationAndRevision(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "d1", Parent: "R", Quota: 2},
		{ID: "d2", Parent: "R", Quota: 2},
		{ID: "g", Parent: "R", Quota: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rs := s.LoanReceipts(); len(rs) != 0 {
		t.Fatalf("receipts = %+v, want empty", rs)
	}

	if _, err := s.TransferLoan("", "d1", "g", 1, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
	if _, err := s.TransferLoan("Lx", "ghost", "g", 1, 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown donor: %v", err)
	}
	if _, err := s.TransferLoan("Lx", "d1", "ghost", 1, 0); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown receiver: %v", err)
	}
	if _, err := s.TransferLoan("Lx", "d1", "g", 0, 0); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("zero amount: %v", err)
	}
	if _, err := s.TransferLoan("Lx", "d1", "g", 1, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale revision: %v", err)
	}
	// 失败的建立不留台账，编号可复用。
	if rs := s.LoanReceipts(); len(rs) != 0 {
		t.Fatalf("receipts after failed transfers = %+v, want empty", rs)
	}

	l1, err := s.TransferLoan("L1", "d1", "g", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if l1.Revision != 1 || l1.Period != 0 || l1.Amount != 1 || l1.Consumed != 0 || l1.Returned != 0 {
		t.Fatalf("L1 = %+v, want revision 1 amount 1 untouched", l1)
	}
	if _, err := s.TransferLoan("L1", "d2", "g", 1, 1); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("duplicate id: %v", err)
	}
	// d1 投影未用只剩 1：超额建立被拒，且不影响已有台账。
	if _, err := s.TransferLoan("L2", "d1", "g", 2, 1); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("exceeds donor unused: %v", err)
	}
	if rs := s.LoanReceipts(); len(rs) != 1 || rs[0].ID != "L1" {
		t.Fatalf("receipts = %+v, want only L1", rs)
	}

	if _, err := s.ReturnLoan("ghost", 1, 1); !errors.Is(err, tenantsched.ErrNotFound) {
		t.Fatalf("unknown loan: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 0, 1); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("zero return: %v", err)
	}
	if _, err := s.ReturnLoan("L1", -1, 1); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("negative return: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 2, 1); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("return beyond amount: %v", err)
	}
	// 参数/状态校验先于修订号比较。
	if _, err := s.ReturnLoan("L1", 2, 99); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("invalid amount before revision: %v", err)
	}
	if _, err := s.ReturnLoan("L1", 1, 99); !errors.Is(err, tenantsched.ErrConflict) {
		t.Fatalf("stale revision return: %v", err)
	}
	// 全部失败调用无副作用：修订号不变、台账不变。
	if s.Revision() != 1 {
		t.Fatalf("revision = %d, want 1 after failed ops", s.Revision())
	}
	if l := s.LoanReceipts()[0]; l.Returned != 0 || l.Revision != 1 {
		t.Fatalf("L1 mutated by failed returns: %+v", l)
	}

	r, err := s.ReturnLoan("L1", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Returned != 1 || r.Revision != 2 {
		t.Fatalf("returned receipt = %+v, want returned 1 revision 2", r)
	}
	if _, err := s.ReturnLoan("L1", 1, 2); !errors.Is(err, tenantsched.ErrInvalidArgument) {
		t.Fatalf("double return of fully returned loan: %v", err)
	}
}
