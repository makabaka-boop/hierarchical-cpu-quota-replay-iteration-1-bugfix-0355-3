package tenantsched

import "fmt"

// LoanReceipt 是一笔可追溯配额借用的台账：Amount 为借用建立时的转让量；
// Consumed 为受让组（或其子组）执行时已按“先基础额度、再按借用提交顺序”
// 归属到本笔的用量；Returned 为已归还量。可归还余额 =
// Amount − Consumed − Returned，别的借用额度不能代替本笔归还。
// Period 为借用生效的周期序号：周期结束借用即终结，旧周期借用不允许再
// 归还（也不可能从新周期基础额度中归还）。Revision 为该台账最近一次
// 变更（建立或归还）时的修订号。
type LoanReceipt struct {
	ID       string
	Donor    string
	Receiver string
	Amount   int
	Returned int
	Consumed int
	Period   uint64
	Revision uint64
}

// TransferLoan 以唯一借用编号 id 建立一笔本周期同父组配额转让
// （donor -> receiver），转让语义与 Transfer 相同，并额外在台账中登记
// 该笔借用：受让组（或其子组）执行消耗超出基础额度时，按借用提交顺序
// 依次计入各笔借用的 Consumed。借用编号全局唯一，建立失败的转让不留台账。
func (s *Scheduler) TransferLoan(id, donor, receiver string, amount int, revision uint64) (LoanReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return LoanReceipt{}, fmt.Errorf("%w: empty loan id", ErrInvalidArgument)
	}
	if _, ok := s.loans[id]; ok {
		return LoanReceipt{}, fmt.Errorf("%w: loan %q already exists", ErrInvalidArgument, id)
	}
	result, err := s.transferLocked(donor, receiver, amount, revision)
	if err != nil {
		return LoanReceipt{}, err
	}
	loan := &LoanReceipt{ID: id, Donor: donor, Receiver: receiver, Amount: amount,
		Period: s.now / PeriodTicks, Revision: result.Revision}
	s.loans[id] = loan
	s.loanOrder = append(s.loanOrder, id)
	// 登记到受让组当前周期的借用序列（提交顺序），供执行扣减时归属用量。
	s.groups[receiver].loans = append(s.groups[receiver].loans, loan)
	return *loan, nil
}

// ReturnLoan 归还借用 id 中尚未使用的额度：amount 不得超过该笔的可归还
// 余额（Amount − Consumed − Returned）——已被消耗的份额不可用别的借用
// 代替归还。借用只在建立它的周期内可归还：周期结束后旧借用终结，归还
// 旧借用会被拒绝，而不会动用新周期的基础额度。归还不退还历史 tick 的
// 扣减，也不改变任何祖先组的额度。
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
	if period := s.now / PeriodTicks; loan.Period != period {
		return LoanReceipt{}, fmt.Errorf("%w: loan %q belongs to terminated period %d, current period %d",
			ErrInvalidArgument, id, loan.Period, period)
	}
	if unused := loan.Amount - loan.Consumed - loan.Returned; amount > unused {
		return LoanReceipt{}, fmt.Errorf("%w: return %d exceeds loan %q unused %d (amount %d, consumed %d, returned %d)",
			ErrInvalidArgument, amount, id, unused, loan.Amount, loan.Consumed, loan.Returned)
	}
	result, err := s.transferLocked(loan.Receiver, loan.Donor, amount, revision)
	if err != nil {
		return LoanReceipt{}, fmt.Errorf("return loan: %w", err)
	}
	loan.Returned += amount
	loan.Revision = result.Revision
	return *loan, nil
}

// LoanReceipts 按借用建立（提交）顺序返回全部借用台账的拷贝，含已终结的
// 历史借用；台账与快照、Applied 轨迹和超期证据使用同一账目。
func (s *Scheduler) LoanReceipts() []LoanReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LoanReceipt, 0, len(s.loanOrder))
	for _, id := range s.loanOrder {
		out = append(out, *s.loans[id])
	}
	return out
}
