package tenantsched_test

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"tenantsched"
)

// ---------- 并发屏障：同一时刻只能有一个 Advance 执行某个 tick ----------

// TestConcurrentAdvanceBarrier 多轮制造“所有 goroutine 在屏障后同时发起
// Advance(1, 同一期望修订号)”的竞争：恰好一个赢家执行该 tick，其余全部
// 以 ErrConflict 被拒绝，且失败调用绝不推进时钟、绝不留下重复轨迹。
func TestConcurrentAdvanceBarrier(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "busy", ReleaseAt: 0, Work: 1_000_000, Deadline: 1_000_000, Group: "R",
	}, 0); err != nil {
		t.Fatal(err)
	}

	const rounds, workers = 25, 16
	for round := 0; round < rounds; round++ {
		var start sync.WaitGroup
		start.Add(1)
		var done sync.WaitGroup
		done.Add(workers)

		var wins, conflicts, other int32
		var winnerTick uint64
		var winMu sync.Mutex
		baseRev := s.Revision()

		for w := 0; w < workers; w++ {
			go func() {
				defer done.Done()
				start.Wait() // 屏障：全部 goroutine 同时竞争
				res, err := s.Advance(1, baseRev)
				switch {
				case err == nil:
					atomic.AddInt32(&wins, 1)
					winMu.Lock()
					winnerTick = res.FromTick
					winMu.Unlock()
				case errors.Is(err, tenantsched.ErrConflict):
					atomic.AddInt32(&conflicts, 1)
				default:
					atomic.AddInt32(&other, 1)
					t.Errorf("unexpected advance error: %v", err)
				}
			}()
		}
		start.Done()
		done.Wait()

		if wins != 1 || conflicts != workers-1 || other != 0 {
			t.Fatalf("round %d: wins=%d conflicts=%d other=%d", round, wins, conflicts, other)
		}
		// 第 round 轮执行的就是 tick round（从 0 起），now 推进到 round+1。
		if got := s.Now(); got != winnerTick+1 || got != uint64(round+1) || winnerTick != uint64(round) {
			t.Fatalf("round %d: now=%d winnerFrom=%d", round, got, winnerTick)
		}
		if s.Revision() != baseRev+1 {
			t.Fatalf("round %d: revision advanced by %d, want 1", round, s.Revision()-baseRev)
		}
	}

	trace := s.Trace()
	if len(trace) != rounds {
		t.Fatalf("trace length %d, want %d", len(trace), rounds)
	}
	seen := map[uint64]bool{}
	for i, e := range trace {
		if e.Tick != uint64(i) {
			t.Fatalf("trace tick gap at %d: %d", i, e.Tick)
		}
		if seen[e.Tick] {
			t.Fatalf("tick %d executed more than once", e.Tick)
		}
		seen[e.Tick] = true
		if e.Kind != tenantsched.TickRan || e.JobID != "busy" {
			t.Fatalf("tick %d expected busy to run, got %s/%s", e.Tick, e.Kind, e.JobID)
		}
	}
}

// TestAdvanceRetryLoop 模拟一批客户端在冲突后读取最新修订号重试：
// 全部 tick 必须仍然只执行一次，最终时钟与赢家次数吻合。
func TestAdvanceRetryLoop(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "busy", ReleaseAt: 0, Work: 1_000_000, Deadline: 1_000_000, Group: "R",
	}, 0); err != nil {
		t.Fatal(err)
	}

	const target, workers = 40, 12
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(workers)

	// 决策锁模拟“每次尝试都在读取修订号的同一临界区里判定是否还需要
	// 推进”，保证不会有 goroutine 拿着陈旧修订号在时钟到达目标后仍成功。
	// 真正的 tick 互斥仍由 Scheduler 自身保证（下面再做独立断言）。
	var decision sync.Mutex
	var success int64
	for w := 0; w < workers; w++ {
		go func() {
			defer done.Done()
			start.Wait()
			for {
				decision.Lock()
				if s.Now() >= target {
					decision.Unlock()
					return
				}
				rev := s.Revision()
				_, err := s.Advance(1, rev)
				decision.Unlock()

				if err == nil {
					atomic.AddInt64(&success, 1)
					continue
				}
				if !errors.Is(err, tenantsched.ErrConflict) {
					t.Errorf("unexpected advance error: %v", err)
					return
				}
			}
		}()
	}
	start.Done()
	done.Wait()

	if got := s.Now(); got != target {
		t.Fatalf("now=%d, want %d", got, target)
	}
	if success != int64(target) {
		t.Fatalf("successful advances=%d, want exactly %d (no duplicate ticks)", success, target)
	}
	// 修订号 = 初始 0 + 1（提交） + 执行 tick 数。
	if s.Revision() != uint64(1+target) {
		t.Fatalf("revision=%d, want %d", s.Revision(), 1+target)
	}
	auditNoDuplicateTicks(t, s.Trace(), 0, target-1)
}

// TestStaleRevisionStorm 更苛刻地检验核心不变量：多个 goroutine 持有
// 各自缓存的陈旧修订号、不经协调反复发起 Advance(1)。这种用法可能超推
// （陈旧客户端不知道别人已经推进），但任何一次成功推进执行的 tick 区间
// 都不得与另一次相交，失败调用必须完全没有副作用。
func TestStaleRevisionStorm(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(tenantsched.JobSpec{
		ID: "busy", ReleaseAt: 0, Work: 1_000_000, Deadline: 1_000_000, Group: "R",
	}, 0); err != nil {
		t.Fatal(err)
	}

	const workers, attemptsEach = 10, 60
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	wg.Add(workers)

	type interval = tickInterval
	var mu sync.Mutex
	var wins []tickInterval
	var conflicts, success int64

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			start.Wait()
			// 该 goroutine 只在开始时读过一次修订号（=1），之后不再刷新，
			// 故意制造陈旧请求；偶发成功后“自以为”的修订号也不更新。
			stale := s.Revision()
			for i := 0; i < attemptsEach; i++ {
				res, err := s.Advance(1, stale)
				if err == nil {
					atomic.AddInt64(&success, 1)
					mu.Lock()
					wins = append(wins, interval{res.FromTick, res.ToTick})
					mu.Unlock()
				} else if errors.Is(err, tenantsched.ErrConflict) {
					atomic.AddInt64(&conflicts, 1)
				} else {
					t.Errorf("unexpected error: %v", err)
					return
				}
			}
		}()
	}
	start.Done()
	wg.Wait()

	if success+conflicts != workers*attemptsEach {
		t.Fatalf("outcome accounting: success=%d conflicts=%d total=%d",
			success, conflicts, workers*attemptsEach)
	}
	if success == 0 {
		t.Fatal("expected at least one successful advance")
	}

	// 成功区间必须两两不相交，且并起来恰好等于 [0, now) 的每个 tick。
	sortIntervals(wins)
	for i := 1; i < len(wins); i++ {
		if wins[i].from != wins[i-1].to {
			t.Fatalf("non-contiguous or overlapping intervals: %+v", wins)
		}
	}
	if wins[0].from != 0 || wins[len(wins)-1].to != s.Now() {
		t.Fatalf("interval coverage [0,%d) vs now=%d", wins[len(wins)-1].to, s.Now())
	}
	if int64(s.Now()-0) != success {
		t.Fatalf("executed ticks=%d but successful calls=%d", s.Now(), success)
	}
	auditNoDuplicateTicks(t, s.Trace(), 0, s.Now()-1)
	for _, e := range s.Trace() {
		if e.Kind != tenantsched.TickRan || e.JobID != "busy" {
			t.Fatalf("tick %d unexpectedly not RAN/busy", e.Tick)
		}
	}
}

type tickInterval struct{ from, to uint64 }

func sortIntervals(xs []tickInterval) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1].from > xs[j].from; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

// TestConcurrentCommitsBarrier 让提交者在屏障后并发提交互不相同的作业，
// 全部使用同一（提交前）期望修订号：恰好一个成功，其余冲突；之后批量
// 推进，所有成功提交都必须出现在某个 tick 的 Applied 中且只出现一次。
func TestConcurrentCommitsBarrier(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{{ID: "R", Quota: 1000}})
	if err != nil {
		t.Fatal(err)
	}

	const committers = 30
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(committers)

	var wins int64
	submitted := make(chan string, committers)
	for i := 0; i < committers; i++ {
		i := i
		go func() {
			defer done.Done()
			start.Wait()
			id := fmt.Sprintf("c%02d", i)
			_, err := s.Submit(tenantsched.JobSpec{
				ID: id, ReleaseAt: 0, Work: 1, Deadline: 1000, Group: "R",
			}, 0)
			if err == nil {
				atomic.AddInt64(&wins, 1)
				submitted <- id
				return
			}
			if !errors.Is(err, tenantsched.ErrConflict) {
				t.Errorf("submit %s unexpected error: %v", id, err)
			}
		}()
	}
	start.Done()
	done.Wait()
	close(submitted)

	if wins != 1 {
		t.Fatalf("concurrent commits: wins=%d, want exactly 1", wins)
	}
	var accepted []string
	for id := range submitted {
		accepted = append(accepted, id)
	}

	// 其余提交者用最新修订号串行补提交。
	for i := 0; i < committers; i++ {
		id := fmt.Sprintf("c%02d", i)
		if id == accepted[0] {
			continue
		}
		if _, err := s.Submit(tenantsched.JobSpec{
			ID: id, ReleaseAt: 0, Work: 1, Deadline: 1000, Group: "R",
		}, s.Revision()); err != nil {
			t.Fatalf("serial submit %s: %v", id, err)
		}
	}
	if len(s.Snapshot().Jobs) != committers {
		t.Fatalf("expected %d jobs, got %d", committers, len(s.Snapshot().Jobs))
	}

	// 推进足够 tick 让所有单 tick 作业完成；每个 tick 只跑一个。
	// 所有提交都发生在推进之前，因此全部在 tick0 开头一次性处理。
	res, err := s.Advance(committers, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Ticks) != committers {
		t.Fatalf("ran %d ticks, want %d", len(res.Ticks), committers)
	}
	if len(res.Ticks[0].Applied) != committers {
		t.Fatalf("tick0 should process all %d commits, got %d",
			committers, len(res.Ticks[0].Applied))
	}

	// 每个提交恰好出现在一个 tick 的 Applied 列表中。
	applied := map[string]int{}
	for _, e := range s.Trace() {
		for _, a := range e.Applied {
			if a.Kind != tenantsched.ChangeSubmit {
				continue
			}
			applied[a.JobID]++
		}
	}
	if len(applied) != committers {
		t.Fatalf("applied submit count=%d, want %d", len(applied), committers)
	}
	for id, n := range applied {
		if n != 1 {
			t.Fatalf("submit of %s applied %d times", id, n)
		}
	}
}

// TestConcurrentTransferBarrier 屏障下多个 goroutine 持同一修订号并发
// 转让：恰好一笔成功，其余全部冲突；失败转让绝不进入 Applied 轨迹，
// 两组有效额度只被赢家改动一次。
func TestConcurrentTransferBarrier(t *testing.T) {
	s, err := tenantsched.New([]tenantsched.GroupSpec{
		{ID: "R", Quota: 1000},
		{ID: "a", Parent: "R", Quota: 10},
		{ID: "b", Parent: "R", Quota: 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 24
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	done.Add(contenders)

	var wins, conflicts int64
	for i := 0; i < contenders; i++ {
		go func() {
			defer done.Done()
			start.Wait()
			// 所有调用持同一修订号 0、同额 1：最多一笔成功。
			_, err := s.Transfer("a", "b", 1, 0)
			switch {
			case err == nil:
				atomic.AddInt64(&wins, 1)
			case errors.Is(err, tenantsched.ErrConflict):
				atomic.AddInt64(&conflicts, 1)
			default:
				t.Errorf("unexpected transfer error: %v", err)
			}
		}()
	}
	start.Done()
	done.Wait()

	if wins != 1 || conflicts != contenders-1 {
		t.Fatalf("wins=%d conflicts=%d, want 1/%d", wins, conflicts, contenders-1)
	}
	if s.Revision() != 1 {
		t.Fatalf("revision=%d, want 1 (failed transfers leave no side effects)", s.Revision())
	}

	// 快照投影：a 有效 9、b 有效 2，尚未入账。
	snap := s.Snapshot()
	eff := map[string]int{}
	for _, q := range snap.Quotas {
		eff[q.GroupID] = q.Effective
	}
	if eff["a"] != 9 || eff["b"] != 2 || eff["R"] != 1000 {
		t.Fatalf("projected effective after race = %v, want a=9 b=2 R=1000", eff)
	}

	// 推进一个 tick：Applied 中恰好一条 TRANSFER，金额 1。
	res, err := s.Advance(1, s.Revision())
	if err != nil {
		t.Fatal(err)
	}
	ap := res.Ticks[0].Applied
	if len(ap) != 1 || ap[0].Kind != tenantsched.ChangeTransfer ||
		ap[0].From != "a" || ap[0].Group != "b" || ap[0].Amount != 1 {
		t.Fatalf("applied = %+v, want exactly one TRANSFER a->b x1", ap)
	}

	// 全量轨迹中该转让只出现一次。
	n := 0
	for _, e := range s.Trace() {
		for _, a := range e.Applied {
			if a.Kind == tenantsched.ChangeTransfer {
				n++
			}
		}
	}
	if n != 1 {
		t.Fatalf("transfer recorded %d times in trace, want 1", n)
	}
}

// TestTransferVsAdvanceRevisionRace 转让与推进持同一旧修订号竞争：
// 两类写操作之间最多成功一笔；若赢家是推进，则转让必须冲突且其调整绝不
// 出现在该 tick（阻挡按没有转让的有效额度重放）。
func TestTransferVsAdvanceRevisionRace(t *testing.T) {
	for round := 0; round < 50; round++ {
		s, err := tenantsched.New([]tenantsched.GroupSpec{
			{ID: "R", Quota: 100},
			{ID: "a", Parent: "R", Quota: 10},
			{ID: "b", Parent: "R", Quota: 1},
			{ID: "c", Parent: "b", Quota: 10},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Submit(tenantsched.JobSpec{
			ID: "jb", ReleaseAt: 0, Work: 5, Deadline: 100, Group: "c",
		}, 0); err != nil {
			t.Fatal(err)
		}
		// 先让 b 用掉基础额度 1（tick0 执行，tick1 起被 b 挡住）。
		if _, err := s.Advance(1, s.Revision()); err != nil {
			t.Fatal(err)
		}
		baseRev := s.Revision() // = 2（1 次提交 + 1 个 tick）

		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup
		wg.Add(2)
		var transferOK, advanceOK int32
		go func() {
			defer wg.Done()
			start.Wait()
			if _, err := s.Transfer("a", "b", 2, baseRev); err == nil {
				atomic.StoreInt32(&transferOK, 1)
			} else if !errors.Is(err, tenantsched.ErrConflict) {
				t.Errorf("transfer unexpected error: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			start.Wait()
			if _, err := s.Advance(1, baseRev); err == nil {
				atomic.StoreInt32(&advanceOK, 1)
			} else if !errors.Is(err, tenantsched.ErrConflict) {
				t.Errorf("advance unexpected error: %v", err)
			}
		}()
		start.Done()
		wg.Wait()

		if transferOK+advanceOK != 1 {
			t.Fatalf("round %d: transferOK=%d advanceOK=%d, want exactly one winner",
				round, transferOK, advanceOK)
		}
		if s.Revision() != baseRev+1 {
			t.Fatalf("round %d: revision=%d, want %d", round, s.Revision(), baseRev+1)
		}
		// 补推进到稳定状态后重放审计：无论谁先赢，账目都必须自洽。
		for s.Now() < 5 {
			if _, err := s.Advance(1, s.Revision()); err != nil {
				t.Fatal(err)
			}
		}
		auditTransferReplay(t, s, []tenantsched.GroupSpec{
			{ID: "R", Parent: "", Quota: 100},
			{ID: "a", Parent: "R", Quota: 10},
			{ID: "b", Parent: "R", Quota: 1},
			{ID: "c", Parent: "b", Quota: 10},
		})
	}
}

// auditTransferReplay 把含转让的轨迹当事件流独立重放：维护每组的本周期
// 转让净额，按“基础额度+净额”为有效额度核对扣减与阻挡，并校验周期归零、
// 修订号 = 变更数（含转让）+ tick 数、每 tick 连续。
func auditTransferReplay(t *testing.T, s *tenantsched.Scheduler, groups []tenantsched.GroupSpec) {
	t.Helper()
	trace := s.Trace()
	snap := s.Snapshot()

	base := map[string]int{}
	parent := map[string]string{}
	for _, g := range groups {
		base[g.ID] = g.Quota
		parent[g.ID] = g.Parent
	}
	used := map[string]int{}
	adjust := map[string]int{}

	var ticks, changes uint64
	transferSeen := map[string]bool{}
	for _, e := range trace {
		ticks++
		if e.Tick%tenantsched.PeriodTicks == 0 {
			for k := range used {
				used[k] = 0
			}
			for k := range adjust {
				adjust[k] = 0
			}
		}
		for _, a := range e.Applied {
			changes++
			if a.Revision == 0 {
				t.Fatalf("tick %d applied change with zero revision", e.Tick)
			}
			if a.Kind == tenantsched.ChangeTransfer {
				if a.Amount <= 0 || a.From == "" || parent[a.From] != parent[a.Group] {
					t.Fatalf("tick %d malformed transfer: %+v", e.Tick, a)
				}
				key := fmt.Sprintf("%s:%s:%d", a.From, a.Group, a.Revision)
				if transferSeen[key] {
					t.Fatalf("transfer %s recorded twice", key)
				}
				transferSeen[key] = true
				adjust[a.From] -= a.Amount
				adjust[a.Group] += a.Amount
				// 转让只动兄弟两组：投影式校验父组净额始终为 0。
				if p := parent[a.From]; p != "" {
					if adjust[p] != 0 {
						t.Fatalf("transfer affected ancestor %s adjust=%d", p, adjust[p])
					}
				}
			}
		}
		if e.Kind == tenantsched.TickRan {
			for _, gid := range e.Path {
				used[gid]++
				eff := base[gid] + adjust[gid]
				if used[gid] > eff {
					t.Fatalf("tick %d group %s used %d exceeds effective %d (base=%d adjust=%d)",
						e.Tick, gid, used[gid], eff, base[gid], adjust[gid])
				}
			}
		}
		if e.Kind == tenantsched.TickIdleBlocked {
			eff := base[e.BlockedBy] + adjust[e.BlockedBy]
			if used[e.BlockedBy] < eff {
				t.Fatalf("tick %d blocker %s not exhausted: used=%d effective=%d",
					e.Tick, e.BlockedBy, used[e.BlockedBy], eff)
			}
		}
	}

	if snap.Revision != changes+ticks {
		t.Fatalf("revision=%d want changes(%d)+ticks(%d)", snap.Revision, changes, ticks)
	}
	// 快照有效额度必须与重放的净额一致。
	for _, q := range snap.Quotas {
		if q.Effective != base[q.GroupID]+adjust[q.GroupID] {
			t.Fatalf("snapshot %s effective=%d replay(base=%d adjust=%d)=%d",
				q.GroupID, q.Effective, base[q.GroupID], adjust[q.GroupID],
				base[q.GroupID]+adjust[q.GroupID])
		}
		if q.Used != used[q.GroupID] {
			t.Fatalf("snapshot %s used=%d replay=%d", q.GroupID, q.Used, used[q.GroupID])
		}
	}
}

// ---------- 轨迹审计：把产品轨迹当成事件流独立重放 ----------

// auditNoDuplicateTicks 校验轨迹恰好覆盖 [from,to]，每个 tick 一条。
func auditNoDuplicateTicks(t *testing.T, trace []tenantsched.TraceEntry, from, to uint64) {
	t.Helper()
	if uint64(len(trace)) != to-from+1 {
		t.Fatalf("audit: trace length %d, want %d", len(trace), to-from+1)
	}
	seen := map[uint64]bool{}
	for i, e := range trace {
		want := from + uint64(i)
		if e.Tick != want || seen[e.Tick] {
			t.Fatalf("audit: tick %d at position %d (want %d, seen=%v)", e.Tick, i, want, seen[e.Tick])
		}
		seen[e.Tick] = true
	}
}

func joinPath(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ">"
		}
		out += x
	}
	return out
}

// TestTraceReplayAudit 在多层配额树上构造确定性脚本，再把返回轨迹当作
// 事件流独立重放，校验：
//   - tick0/1 执行 jA，b 与 a 在 tick1 末同时耗尽；tick1 是 jA 的截止
//     tick，仍欠 2 个工作量 -> 超期证据，最深阻挡祖先为 b；
//   - tick2..9 唯一就绪作业 jA 被 b 挡住（IDLE_BLOCKED，周期内不恢复）；
//   - tick10 周期边界全部归零，jA 在 tick10/11 恢复执行并完成；
//   - tick12/13 后释放的 jB 经 a>R 调度完成；tick14 无可运行作业；
//   - tick15 开头处理 MIGRATE（作业尚未释放），tick16..19 仍空转；
//   - tick20 第二个周期边界重置，已释放的 jC 沿新路径 b>a>R 逐级扣减
//     并完成；tick21..24 空转；
//   - 每级 Used 不超过 Quota、扣减值与重放账目逐点一致；
//   - 修订号 = 提交变更数 + tick 数，轨迹 tick 连续无重复。
func TestTraceReplayAudit(t *testing.T) {
	// R 在每个 10 tick 周期内都不会成为瓶颈；b 先于 a 耗尽，周期重置后
	// 新路径 b>a>R 仍有容量接纳迁移后的 jC。
	groups := []tenantsched.GroupSpec{
		{ID: "R", Parent: "", Quota: 100},
		{ID: "a", Parent: "R", Quota: 4},
		{ID: "b", Parent: "a", Quota: 2},
	}
	s, err := tenantsched.New(groups)
	if err != nil {
		t.Fatal(err)
	}

	rev := uint64(0)
	submit := func(j tenantsched.JobSpec) {
		t.Helper()
		r, err := s.Submit(j, rev)
		if err != nil {
			t.Fatalf("submit %s: %v", j.ID, err)
		}
		rev = r.Revision
	}
	advance := func(n int) tenantsched.AdvanceResult {
		t.Helper()
		r, err := s.Advance(n, rev)
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
		rev = r.Revision
		return r
	}

	submit(tenantsched.JobSpec{ID: "jA", ReleaseAt: 0, Work: 4, Deadline: 1, Group: "b"})
	submit(tenantsched.JobSpec{ID: "jB", ReleaseAt: 12, Work: 2, Deadline: 15, Group: "a"})
	submit(tenantsched.JobSpec{ID: "jC", ReleaseAt: 20, Work: 1, Deadline: 30, Group: "R"})

	r1 := advance(2) // tick0/1 jA 连跑；tick1 结束时仍欠 2 -> 截止超期
	if r1.Ticks[0].Kind != tenantsched.TickRan || r1.Ticks[0].JobID != "jA" {
		t.Fatalf("tick0 = %+v, want RAN jA", r1.Ticks[0])
	}
	if r1.Ticks[1].Kind != tenantsched.TickRan || r1.Ticks[1].JobID != "jA" {
		t.Fatalf("tick1 = %+v, want RAN jA", r1.Ticks[1])
	}
	if len(r1.Overdue) != 1 {
		t.Fatalf("want 1 overdue at tick1, got %+v", r1.Overdue)
	}
	ev := r1.Overdue[0]
	if ev.JobID != "jA" || ev.Deadline != 1 || ev.DetectedAt != 1 ||
		ev.Remaining != 2 || ev.Group != "b" || ev.Blocker != "b" || ev.State != tenantsched.JobReady {
		t.Fatalf("jA overdue evidence wrong: %+v", ev)
	}

	r2 := advance(8) // tick2..9：b 已耗尽，jA 是唯一就绪作业
	for i, e := range r2.Ticks {
		if e.Kind != tenantsched.TickIdleBlocked || e.BlockedBy != "b" || e.ReadyCount != 1 {
			t.Fatalf("blocked tick[%d] (tick %d) %+v: want IDLE_BLOCKED by b ready=1",
				i, e.Tick, e)
		}
	}

	r3 := advance(2) // tick10/11：周期重置后 jA 跑完剩余 2 个工作量
	for i, e := range r3.Ticks {
		if e.Kind != tenantsched.TickRan || e.JobID != "jA" || e.Period != 1 {
			t.Fatalf("post-reset tick[%d] %+v: want RAN jA in period 1", i, e)
		}
	}

	r4 := advance(2) // tick12/13：jB 释放后经 a>R 连跑两次完成
	if r4.Ticks[0].JobID != "jB" || r4.Ticks[1].JobID != "jB" {
		t.Fatalf("tick12/13 = %s/%s, want jB/jB", r4.Ticks[0].JobID, r4.Ticks[1].JobID)
	}

	r5 := advance(1) // tick14 空转
	if e0 := r5.Ticks[0]; e0.Kind != tenantsched.TickIdleNotReady || e0.ReadyCount != 0 {
		t.Fatalf("tick14 = %+v, want IDLE_NOT_READY", e0)
	}

	// 迁组在 tick15 之前提交：tick15 开头先入账该变更；jC 要到 tick20
	// 才释放，因此 tick15..19 继续空转。
	rm, err := s.Migrate("jC", "b", rev)
	if err != nil {
		t.Fatal(err)
	}
	rev = rm.Revision

	r6 := advance(5) // tick15 处理 MIGRATE；tick15..19 仍无可运行作业
	if ap := r6.Ticks[0].Applied; len(ap) != 1 || ap[0].Kind != tenantsched.ChangeMigrate ||
		ap[0].JobID != "jC" || ap[0].Group != "b" {
		t.Fatalf("tick15 must process jC->b migrate, got %+v", ap)
	}
	for _, e := range r6.Ticks {
		if e.Kind != tenantsched.TickIdleNotReady || e.ReadyCount != 0 {
			t.Fatalf("tick %d = %+v, want IDLE_NOT_READY", e.Tick, e)
		}
	}

	r7 := advance(5) // tick20 第二个周期边界重置，jC 释放后沿新路径执行
	last := r7.Ticks[0]
	if last.Tick != 20 || last.Kind != tenantsched.TickRan || last.JobID != "jC" {
		t.Fatalf("tick20 = %+v, want RAN jC after period reset", last)
	}
	if joinPath(last.Path) != "b>a>R" {
		t.Fatalf("jC migrated path = %v, want b>a>R", last.Path)
	}
	if len(last.Deducted) != 3 {
		t.Fatalf("tick20 deducted=%v, want one entry per ancestor", last.Deducted)
	}
	for i, e := range r7.Ticks[1:] {
		if e.Kind != tenantsched.TickIdleNotReady || e.ReadyCount != 0 {
			t.Fatalf("post-jC tick[%d] (tick %d) %+v: want IDLE_NOT_READY", i, e.Tick, e)
		}
	}

	auditReplay(t, s, groups)
}

// auditReplay 从轨迹独立重放配额与工作量账目并核对快照与修订号。
func auditReplay(t *testing.T, s *tenantsched.Scheduler, groups []tenantsched.GroupSpec) {
	t.Helper()
	trace := s.Trace()
	snap := s.Snapshot()

	quota := map[string]int{}
	for _, g := range groups {
		quota[g.ID] = g.Quota
	}
	used := map[string]int{}
	ranCount := map[string]int{}

	var totalTicks uint64
	var commitCount uint64
	appliedSeen := map[string]bool{}

	for _, e := range trace {
		totalTicks++
		if e.Tick%tenantsched.PeriodTicks == 0 {
			for k := range used {
				used[k] = 0
			}
		}
		for _, a := range e.Applied {
			commitCount++
			if a.Revision == 0 {
				t.Fatalf("tick %d applied change with zero revision", e.Tick)
			}
			if a.Kind == tenantsched.ChangeMigrate && a.Group == "" {
				t.Fatalf("tick %d migrate without target group", e.Tick)
			}
			key := fmt.Sprintf("%s:%s:%d", a.Kind, a.JobID, a.Revision)
			if appliedSeen[key] {
				t.Fatalf("change %s recorded in two ticks", key)
			}
			appliedSeen[key] = true
		}

		switch e.Kind {
		case tenantsched.TickRan:
			if !e.Ran || e.JobID == "" || len(e.Path) != len(e.Deducted) {
				t.Fatalf("tick %d malformed RAN entry: %+v", e.Tick, e)
			}
			if e.BlockedBy != "" {
				t.Fatalf("tick %d RAN with blocker", e.Tick)
			}
			for i, gid := range e.Path {
				used[gid]++
				if e.Deducted[i] != used[gid] {
					t.Fatalf("tick %d deducted[%d]=%d but replay used=%d for %s",
						e.Tick, i, e.Deducted[i], used[gid], gid)
				}
				if used[gid] > quota[gid] {
					t.Fatalf("tick %d group %s used %d > quota %d", e.Tick, gid, used[gid], quota[gid])
				}
			}
			ranCount[e.JobID]++
		case tenantsched.TickIdleBlocked:
			if e.Ran || e.JobID != "" || e.BlockedBy == "" || e.ReadyCount == 0 {
				t.Fatalf("tick %d malformed IDLE_BLOCKED: %+v", e.Tick, e)
			}
			if used[e.BlockedBy] < quota[e.BlockedBy] {
				t.Fatalf("tick %d blocker %s not exhausted: used=%d quota=%d",
					e.Tick, e.BlockedBy, used[e.BlockedBy], quota[e.BlockedBy])
			}
		case tenantsched.TickIdleNotReady:
			if e.Ran || e.JobID != "" || e.BlockedBy != "" || e.ReadyCount != 0 {
				t.Fatalf("tick %d malformed IDLE_NOT_READY: %+v", e.Tick, e)
			}
		default:
			t.Fatalf("tick %d unknown kind %q", e.Tick, e.Kind)
		}
	}

	jobByName := map[string]tenantsched.JobView{}
	for _, j := range snap.Jobs {
		jobByName[j.ID] = j
	}
	for _, j := range snap.Jobs {
		if got, want := ranCount[j.ID], j.Work-j.Remaining; got != want {
			t.Fatalf("job %s ran %d times but work-remaining=%d", j.ID, got, want)
		}
		if j.State == tenantsched.JobCompleted && j.Remaining != 0 {
			t.Fatalf("completed job %s has remaining %d", j.ID, j.Remaining)
		}
		if j.State == tenantsched.JobCanceled && j.Overdue {
			t.Fatalf("canceled job %s marked overdue", j.ID)
		}
	}

	for _, q := range snap.Quotas {
		if q.Used != used[q.GroupID] {
			t.Fatalf("quota %s snapshot used=%d replay=%d", q.GroupID, q.Used, used[q.GroupID])
		}
		if q.Used > q.Effective {
			t.Fatalf("quota %s over limit: %d/%d (base %d)", q.GroupID, q.Used, q.Effective, q.Quota)
		}
	}

	for _, ov := range s.Overdue() {
		j, ok := jobByName[ov.JobID]
		if !ok {
			t.Fatalf("overdue evidence for unknown job %s", ov.JobID)
		}
		if ov.DetectedAt != ov.Deadline || ov.Remaining <= 0 {
			t.Fatalf("bad overdue evidence: %+v", ov)
		}
		ranByDeadline := 0
		for _, x := range trace {
			if x.Tick <= ov.Deadline && x.JobID == ov.JobID {
				ranByDeadline++
			}
		}
		if ov.Remaining != j.Work-ranByDeadline {
			t.Fatalf("overdue %s remaining=%d but work=%d ranByDeadline=%d",
				ov.JobID, ov.Remaining, j.Work, ranByDeadline)
		}
	}

	if want := commitCount + totalTicks; snap.Revision != want {
		t.Fatalf("revision=%d, want commits(%d)+ticks(%d)=%d", snap.Revision, commitCount, totalTicks, want)
	}
	if snap.Now != totalTicks {
		t.Fatalf("snapshot now=%d ticks=%d", snap.Now, totalTicks)
	}
	auditNoDuplicateTicks(t, trace, 0, totalTicks-1)
}
