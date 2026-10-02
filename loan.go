package tenantsched

import "fmt"

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

func (s *Scheduler) TransferLoan(id, donor, receiver string, amount int, revision uint64) (LoanReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return LoanReceipt{}, ErrInvalidArgument
	}
	if s.loans == nil {
		s.loans = map[string]*LoanReceipt{}
	}
	if _, ok := s.loans[id]; ok {
		return LoanReceipt{}, ErrInvalidArgument
	}
	result, err := s.transferLocked(donor, receiver, amount, revision)
	if err != nil {
		return LoanReceipt{}, err
	}
	loan := &LoanReceipt{ID: id, Donor: donor, Receiver: receiver, Amount: amount,
		Period: s.now / PeriodTicks, Revision: result.Revision}
	s.loans[id] = loan
	return *loan, nil
}

func (s *Scheduler) ReturnLoan(id string, amount int, revision uint64) (LoanReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	loan := s.loans[id]
	if loan == nil {
		return LoanReceipt{}, ErrNotFound
	}
	if amount <= 0 || amount > loan.Amount-loan.Returned {
		return LoanReceipt{}, ErrInvalidArgument
	}
	result, err := s.transferLocked(loan.Receiver, loan.Donor, amount, revision)
	if err != nil {
		return LoanReceipt{}, fmt.Errorf("return loan: %w", err)
	}
	loan.Returned += amount
	loan.Revision = result.Revision
	return *loan, nil
}

func (s *Scheduler) LoanReceipts() []LoanReceipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []LoanReceipt{}
	for _, loan := range s.loans {
		out = append(out, *loan)
	}
	return out
}
