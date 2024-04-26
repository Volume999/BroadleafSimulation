package simulator

import "math/rand"

type WithContention struct {
	simulator Simulator
	lockCnt   int
	locks     []chan struct{}
}

func NewWithContention(simulator Simulator, lockCount int) *WithContention {
	locks := make([]chan struct{}, lockCount)
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
	}
	return &WithContention{
		simulator: simulator,
		lockCnt:   lockCount,
		locks:     locks,
	}
}

func (s *WithContention) acquireLock(lockIndex int) {
	s.locks[lockIndex] <- struct{}{}
}

func (s *WithContention) releaseLock(lockIndex int) {
	<-s.locks[lockIndex]
}

func (s *WithContention) withLock(f func()) {
	lockIndex := rand.Intn(s.lockCnt)
	s.acquireLock(lockIndex)
	defer s.releaseLock(lockIndex)
	f()
}

func (s *WithContention) ValidateCheckout() {
	s.withLock(s.simulator.ValidateCheckout)
}

func (s *WithContention) ValidateAvailability() {
	s.withLock(s.simulator.ValidateAvailability)
}

func (s *WithContention) VerifyCustomer() {
	s.withLock(s.simulator.VerifyCustomer)
}

func (s *WithContention) ValidatePayment() {
	s.withLock(s.simulator.ValidatePayment)
}

func (s *WithContention) ValidateProductOption() {
	s.withLock(s.simulator.ValidateProductOption)
}

func (s *WithContention) RecordOffer() {
	s.withLock(s.simulator.RecordOffer)
}

func (s *WithContention) CommitTax() {
	s.withLock(s.simulator.CommitTax)
}

func (s *WithContention) DecrementInventory() {
	s.withLock(s.simulator.DecrementInventory)
}

func (s *WithContention) CompleteOrder() {
	s.withLock(s.simulator.CompleteOrder)
}
