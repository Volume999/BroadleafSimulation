package simulator

import (
	"github.com/Volume999/BroadleafSimulation/workload"
	"math/rand"
)

type SequentialSimulator struct {
	config *Config
	disk   workload.DiskAccessSimulator
}

func NewSequentialSimulator(config *Config, disk workload.DiskAccessSimulator) *SequentialSimulator {
	return &SequentialSimulator{
		config: config,
		disk:   disk,
	}
}

func (s *SequentialSimulator) ValidateCheckout() {
	// This function was not implemented in the original BroadLeaf use-case
}

func (s *SequentialSimulator) ValidateAvailability() {
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		s.disk.SimulateDiskAccess()   // Load to get the item availability
		workload.SimulateCpuLoad(100) // Merge SKU Items
	}
	skuItemsCnt := s.config.SKUItemsCnt
	for range skuItemsCnt {
		s.disk.SimulateDiskAccess()   // Load to get the SKU availability
		workload.SimulateCpuLoad(100) // Some operations on SKU Items
	}
}

func (s *SequentialSimulator) VerifyCustomer() {
	s.disk.SimulateDiskAccess() // Load to get the customer details
	workload.SimulateCpuLoad(100)
	appliedOffersCnt := s.config.AppliedOffersCnt
	for range appliedOffersCnt {
		isLimitedUse := rand.Intn(2) == 0
		if isLimitedUse {
			s.disk.SimulateDiskAccess() // Get uses by customer
			workload.SimulateCpuLoad(1000)
		}
	}
}

func (s *SequentialSimulator) ValidatePayment() {
	s.disk.SimulateDiskAccess() // Get Order
	paymentsCnt := s.config.PaymentsCnt
	for range paymentsCnt {
		isActive := rand.Intn(10) < 4
		if isActive {
			s.disk.SimulateDiskAccess() // Make new transaction
			workload.SimulateCpuLoad(10000)
			s.disk.SimulateDiskAccess()
			s.disk.SimulateDiskAccess()
		}
	}
}

func (s *SequentialSimulator) ValidateProductOption() {

}

func (s *SequentialSimulator) RecordOffer() {
	s.disk.SimulateDiskAccess() // Get Order
	workload.SimulateCpuLoad(10000)
}

func (s *SequentialSimulator) CommitTax() {
	s.disk.SimulateDiskAccess() // Get Order
	s.disk.SimulateDiskAccess()
}

func (s *SequentialSimulator) DecrementInventory() {
	s.disk.SimulateDiskAccess()
	orderItemsCnt := s.config.OrderItemsCnt
	for range orderItemsCnt {
		s.disk.SimulateDiskAccess()    // put Item
		workload.SimulateCpuLoad(1000) // Merge SKU Items
		s.disk.SimulateDiskAccess()    // put SKU
	}
}

func (s *SequentialSimulator) CompleteOrder() {
	s.disk.SimulateDiskAccess()
}
