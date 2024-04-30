package simulator

import (
	"github.com/Volume999/BroadleafSimulation/workload"
	"math/rand"
	"sync"
)

type AsyncSimulator struct {
	config *Config
	disk   workload.DiskAccessSimulator
}

func NewAsyncSimulator(config *Config, disk workload.DiskAccessSimulator) *AsyncSimulator {
	return &AsyncSimulator{
		config: config,
		disk:   disk,
	}
}

func (s *AsyncSimulator) ValidateCheckout() {
}

func (s *AsyncSimulator) ValidateAvailability() {
	orderItemsCnt := s.config.OrderItemsCnt
	wg := &sync.WaitGroup{}
	wg.Add(orderItemsCnt)
	for range orderItemsCnt {
		go func() {
			defer wg.Done()
			s.disk.SimulateDiskAccess()
			workload.SimulateCpuLoad(100)
		}()
	}
	wg.Wait()
	skuItemsCnt := s.config.SKUItemsCnt
	wg.Add(skuItemsCnt)
	for range skuItemsCnt {
		go func() {
			defer wg.Done()
			s.disk.SimulateDiskAccess()
			workload.SimulateCpuLoad(100)
		}()
	}
	wg.Wait()
	workload.SimulateCpuLoad(10000)
}

func (s *AsyncSimulator) VerifyCustomer() {
	wg := &sync.WaitGroup{}
	s.disk.SimulateDiskAccess()
	workload.SimulateCpuLoad(100)
	appliedOffersCnt := s.config.AppliedOffersCnt
	wg.Add(appliedOffersCnt)
	for range appliedOffersCnt {
		go func() {
			defer wg.Done()
			isLimitedUse := rand.Intn(2) == 0
			if isLimitedUse {
				s.disk.SimulateDiskAccess()
				workload.SimulateCpuLoad(1000)
			}
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) ValidatePayment() {
	s.disk.SimulateDiskAccess()
	paymentsCnt := s.config.PaymentsCnt
	wg := &sync.WaitGroup{}
	wg.Add(paymentsCnt)
	for range paymentsCnt {
		go func() {
			defer wg.Done()
			isActive := rand.Intn(10) < 4
			if isActive {
				s.disk.SimulateDiskAccess()
				workload.SimulateCpuLoad(10000)
				s.disk.SimulateDiskAccess()
				s.disk.SimulateDiskAccess()
			}
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) ValidateProductOption() {

}

func (s *AsyncSimulator) RecordOffer() {
	s.disk.SimulateDiskAccess()
	workload.SimulateCpuLoad(10000)
}

func (s *AsyncSimulator) CommitTax() {
	s.disk.SimulateDiskAccess()
	s.disk.SimulateDiskAccess()
}

func (s *AsyncSimulator) DecrementInventory() {
	s.disk.SimulateDiskAccess()
	orderItemsCnt := s.config.OrderItemsCnt
	wg := &sync.WaitGroup{}
	wg.Add(orderItemsCnt)
	for range orderItemsCnt {
		go func() {
			defer wg.Done()
			s.disk.SimulateDiskAccess()
			workload.SimulateCpuLoad(1000)
			s.disk.SimulateDiskAccess()
		}()
	}
	wg.Wait()
}

func (s *AsyncSimulator) CompleteOrder() {
	s.disk.SimulateDiskAccess()
}
