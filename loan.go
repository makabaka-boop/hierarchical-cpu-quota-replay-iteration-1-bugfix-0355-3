package tenantsched

import "fmt"

// LoanReceipt 是一笔可追溯配额借用的回执。借用通过 TransferLoan 以唯一
// 编号建立，本质是本周期的同父组配额转让；ReturnLoan 只归还该笔尚未使用
// 的余额。回执与逐 tick 轨迹（Applied 中携带 LoanID 的 TRANSFER 变更）、
// 快照和超期证据口径一致，可逐笔核对。
type LoanReceipt struct {
	ID       string
	Donor    string
	Receiver string
	Amount   int
	// Returned 是本笔已归还总量；Consumed 是本周期内受让组（含其子组的
	// 作业）执行时，按“先消耗自有（非借用）有效额度、再按借用提交顺序”
	// 的规则归属到本笔的已消耗量。可归还余额 = Amount - Returned -
	// Consumed；其他借用的额度不能代替本笔已消耗的余额归还。
	Returned int
	Consumed int
	// Period 是借用生效的周期序号。周期结束时本笔借用终结：未归还余额
	// 随周期重置失效，旧借用不得再从新周期的基础额度中归还。
	Period uint64
	// Revision 是本笔最近一次变更（建立或归还）提交成功时的修订号。
	Revision uint64
}

// TransferLoan 以唯一借用编号 id 建立一笔本周期同父组配额借用：amount 从
// donor 的本周期有效额度划出、加到 receiver。参数校验、修订号检查与入账
// 规则同 Transfer（提交即生效、下一个 tick 入账；边界临界提交属于新周期）。
// 借用编号全局唯一、不可复用。
func (s *Scheduler) TransferLoan(id, donor, receiver string, amount int, revision uint64) (LoanReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return LoanReceipt{}, fmt.Errorf("%w: empty loan id", ErrInvalidArgument)
	}
	if s.loans == nil {
		s.loans = map[string]*LoanReceipt{}
	}
	if _, ok := s.loans[id]; ok {
		return LoanReceipt{}, fmt.Errorf("%w: loan id %q already exists", ErrInvalidArgument, id)
	}
	result, err := s.transferLocked(donor, receiver, amount, revision, id)
	if err != nil {
		return LoanReceipt{}, err
	}
	loan := &LoanReceipt{ID: id, Donor: donor, Receiver: receiver, Amount: amount,
		Period: s.now / PeriodTicks, Revision: result.Revision}
	s.loans[id] = loan
	s.loanOrder = append(s.loanOrder, loan)
	// 自下一个 tick 起参与受让组的消耗归属；边界临界提交的借用属于新
	// 周期，周期重置会按 Period 保留它。
	s.groups[receiver].activeLoans = append(s.groups[receiver].activeLoans, loan)
	return *loan, nil
}

// ReturnLoan 归还借用 id 的未使用余额：amount 必须为正且不超过
// Amount - Returned - Consumed。已被消耗的部分不可归还，其他借用的额度
// 也不能代替本笔归还；周期结束后旧借用一律终结，不得再从新周期的基础
// 额度中归还。归还只改两组本周期有效额度：不退还历史 tick 扣减，不改变
// 父组与任何祖先组的额度。
func (s *Scheduler) ReturnLoan(id string, amount int, revision uint64) (LoanReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	loan := s.loans[id]
	if loan == nil {
		return LoanReceipt{}, fmt.Errorf("%w: unknown loan %q", ErrNotFound, id)
	}
	if amount <= 0 {
		return LoanReceipt{}, fmt.Errorf("%w: return amount must be positive, got %d", ErrInvalidArgument, amount)
	}
	// 周期结束所有旧借用终结：不允许跨周期从新周期额度中归还旧借用。
	if loan.Period != s.now/PeriodTicks {
		return LoanReceipt{}, fmt.Errorf("%w: loan %q expired at end of period %d",
			ErrInvalidArgument, id, loan.Period)
	}
	// 只归还本笔尚未使用的余额；已消耗部分与其他借用的额度都不可顶替。
	if unused := loan.Amount - loan.Returned - loan.Consumed; amount > unused {
		return LoanReceipt{}, fmt.Errorf("%w: return %d exceeds loan %q unused %d",
			ErrInvalidArgument, amount, id, unused)
	}
	result, err := s.transferLocked(loan.Receiver, loan.Donor, amount, revision, id)
	if err != nil {
		return LoanReceipt{}, fmt.Errorf("return loan: %w", err)
	}
	loan.Returned += amount
	loan.Revision = result.Revision
	return *loan, nil
}

// LoanReceipts 按建立（提交）顺序返回全部借用回执的拷贝，包括已终结的
// 旧周期借用；与 Snapshot、Applied 轨迹和超期证据口径一致。
func (s *Scheduler) LoanReceipts() []LoanReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LoanReceipt, 0, len(s.loanOrder))
	for _, loan := range s.loanOrder {
		out = append(out, *loan)
	}
	return out
}
