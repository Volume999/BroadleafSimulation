package workflows

import (
	"github.com/Volume999/BroadleafSimulation/simulator"
	"sync"
)

type AsyncWorkflow struct {
	s simulator.Simulator
}

func NewAsyncWorkflow(s simulator.Simulator) *AsyncWorkflow {
	return &AsyncWorkflow{s}
}

func (w *AsyncWorkflow) Execute() {
	validationPhase := []func(){
		w.s.ValidateCheckout,
		w.s.ValidateAvailability,
		w.s.VerifyCustomer,
		w.s.ValidatePayment,
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()

	operationPhase := []func(){
		w.s.RecordOffer,
		w.s.CommitTax,
		w.s.DecrementInventory,
	}
	wg.Add(len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()
	w.s.CompleteOrder()
}
