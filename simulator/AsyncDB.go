package simulator

import (
	"errors"
	"github.com/Volume999/AsyncDB/asyncdb"
	simulation "github.com/Volume999/BroadleafSimulation"
	"github.com/Volume999/BroadleafSimulation/workload"
	"math/rand"
)

var ErrBusinessLogic = errors.New("business logic error")

type TableReadWriteSimulator interface {
	ReadN(string, int) error
	WriteN(string, int) error
}

type AsyncDBSimulator struct {
	db     *asyncdb.AsyncDB
	ctx    *asyncdb.ConnectionContext
	rw     TableReadWriteSimulator
	config *simulation.Config
	keys   int
}

func RandomIntInRange(i int, i2 int) int {
	return rand.Intn(i2-i+1) + i
}

func RandomChance(prob int) bool {
	return rand.Intn(100) < prob
}

func (a AsyncDBSimulator) ValidateCheckout() error {
	workload.SimulateCpuLoad(100)
	// One DB call for checking isCompleted
	if err := a.rw.ReadN("Orders", 1); err != nil {
		return err
	}
	if RandomChance(3) {
		return ErrBusinessLogic
	}
	return nil
}

func (a AsyncDBSimulator) ValidateAvailability() error {
	// Get the Item records
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	// Get SKU Records
	if err := a.rw.ReadN("StockKeeingUnits", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(3) {
		return ErrBusinessLogic
	}
	return nil
}

func (a AsyncDBSimulator) VerifyCustomer() error {
	// Try to read customer	record
	if err := a.rw.ReadN("Customers", 1); err != nil {
		return err
	}

	// Read offers, assume one offer per order item
	if err := a.rw.ReadN("ItemOffers", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(3) {
		return ErrBusinessLogic
	}
	return nil
}

func (a AsyncDBSimulator) ValidatePayment() error {
	// Check payments, confirm unconfirmed payments
	// Assume 1 or 2 payments are unconfirmed
	var confirmedPayments, unconfirmedPayments int
	unconfirmedPayments = min(a.config.PaymentsCnt, 2)
	confirmedPayments = a.config.PaymentsCnt - unconfirmedPayments
	if err := a.rw.ReadN("OrderPayments", confirmedPayments); err != nil {
		return err
	}
	if err := a.rw.WriteN("OrderPayments", unconfirmedPayments); err != nil {
		return err
	}
	if RandomChance(3) {
		return ErrBusinessLogic
	}
	return nil
}

func (a AsyncDBSimulator) ValidateProductOption() error {
	// Assume one ItemOption per OrderItem
	if err := a.rw.ReadN("ItemOptions", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(3) {
		return ErrBusinessLogic
	}
	return nil
}

func (a AsyncDBSimulator) RecordOffer() error {
	if err := a.rw.WriteN("CustomerOffersUsage", a.config.OrderItemsCnt); err != nil {
		return err
	}
	return nil
}

func (a AsyncDBSimulator) CommitTax() error {
	if err := a.rw.ReadN("TaxProviders", 1); err != nil {
		return err
	}
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if err := a.rw.WriteN("OrderTaxes", 1); err != nil {
		return err
	}
	return nil
}

func (a AsyncDBSimulator) DecrementInventory() error {
	// TODO: These reads and writes should hit the same keys as other activities
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if err := a.rw.WriteN("StockKeepingUnits", a.config.OrderItemsCnt); err != nil {
		return err
	}
	return nil
}

func (a AsyncDBSimulator) CompleteOrder() error {
	// TODO: In general, I should initialize access keys before I start the simulation for better lock integrity
	if err := a.rw.WriteN("Orders", 1); err != nil {
		return err
	}
	return nil
}

func NewAsyncDBSimulator(db *asyncdb.AsyncDB, ctx *asyncdb.ConnectionContext, rw TableReadWriteSimulator, config *simulation.Config, keys int) *AsyncDBSimulator {
	return &AsyncDBSimulator{
		db:     db,
		ctx:    ctx,
		rw:     rw,
		config: config,
		keys:   keys,
	}
}
