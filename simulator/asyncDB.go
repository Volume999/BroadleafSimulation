package simulator

import (
	"errors"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/workload"
	"log"
	"math/rand"
)

var ErrBusinessLogic = errors.New("business logic error")

type TableReadWriteSimulator interface {
	ReadN(string, int) error
	WriteN(string, int) error
}

type ConcTableReadWriteSimulator struct {
	db   *asyncdb.AsyncDB
	ctx  *asyncdb.ConnectionContext
	keys int
}

func NewConcTableReadWriteSimulator(db *asyncdb.AsyncDB, ctx *asyncdb.ConnectionContext, keys int) *ConcTableReadWriteSimulator {
	return &ConcTableReadWriteSimulator{db: db, ctx: ctx, keys: keys}
}

func (c ConcTableReadWriteSimulator) ReadN(table string, n int) error {
	var readErr error
	errChan := make(chan error, n)
	defer close(errChan)
	for i := 0; i < n; i++ {
		key := RandomIntInRange(1, c.keys)
		go func() {
			res := <-c.db.Get(c.ctx, table, key)
			errChan <- res.Err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errChan; err != nil {
			readErr = errors.Join(readErr, err)
		}
	}
	return readErr
}

func (c ConcTableReadWriteSimulator) WriteN(table string, n int) error {
	var writeErr error
	errChan := make(chan error, n)
	defer close(errChan)
	for i := 0; i < n; i++ {
		key := RandomIntInRange(1, c.keys)
		go func() {
			res := <-c.db.Put(c.ctx, table, key, "value")
			errChan <- res.Err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errChan; err != nil {
			writeErr = errors.Join(writeErr, err)
		}
	}
	return writeErr
}

type SyncTableReadWriteSimulator struct {
	db   *asyncdb.AsyncDB
	ctx  *asyncdb.ConnectionContext
	keys int
}

func NewSyncTableReadWriteSimulator(db *asyncdb.AsyncDB, ctx *asyncdb.ConnectionContext, keys int) *SyncTableReadWriteSimulator {
	return &SyncTableReadWriteSimulator{db: db, ctx: ctx, keys: keys}
}

func (s SyncTableReadWriteSimulator) ReadN(table string, n int) error {
	for i := 0; i < n; i++ {
		key := RandomIntInRange(1, s.keys)
		res := <-s.db.Get(s.ctx, table, key)
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

func (s SyncTableReadWriteSimulator) WriteN(table string, n int) error {
	for i := 0; i < n; i++ {
		key := RandomIntInRange(1, s.keys)
		res := <-s.db.Put(s.ctx, table, key, "value")
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

type AsyncDBSimulator struct {
	rw              TableReadWriteSimulator
	l               *log.Logger
	config          *Config
	keys            int
	businessErrProb int
}

func RandomIntInRange(i int, i2 int) int {
	return rand.Intn(i2-i+1) + i
}

func RandomChance(prob int) bool {
	return rand.Intn(100) < prob
}

func (a *AsyncDBSimulator) ValidateCheckout() error {
	workload.SimulateCpuLoad(100)
	// One DB call for checking isCompleted
	if err := a.rw.ReadN("Orders", 1); err != nil {
		return err
	}
	if RandomChance(a.businessErrProb) {
		return ErrBusinessLogic
	}
	return nil
}

func (a *AsyncDBSimulator) ValidateAvailability() error {
	// Get the Item records
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	// Get SKU Records
	if err := a.rw.ReadN("StockKeepingUnits", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(a.businessErrProb) {
		return ErrBusinessLogic
	}
	return nil
}

func (a *AsyncDBSimulator) VerifyCustomer() error {
	// Try to read customer	record
	if err := a.rw.ReadN("Customers", 1); err != nil {
		return err
	}

	// Read offers, assume one offer per order item
	if err := a.rw.ReadN("ItemOffers", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(a.businessErrProb) {
		return ErrBusinessLogic
	}
	return nil
}

func (a *AsyncDBSimulator) ValidatePayment() error {
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
	if RandomChance(a.businessErrProb) {
		return ErrBusinessLogic
	}
	return nil
}

func (a *AsyncDBSimulator) ValidateProductOption() error {
	// Assume one ItemOption per OrderItem
	if err := a.rw.ReadN("ItemOptions", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if RandomChance(a.businessErrProb) {
		return ErrBusinessLogic
	}
	return nil
}

func (a *AsyncDBSimulator) RecordOffer() error {
	if err := a.rw.WriteN("CustomerOffersUsage", a.config.OrderItemsCnt); err != nil {
		return err
	}
	return nil
}

func (a *AsyncDBSimulator) CommitTax() error {
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if err := a.rw.WriteN("OrderTaxes", 1); err != nil {
		return err
	}
	return nil
}

func (a *AsyncDBSimulator) DecrementInventory() error {
	// TODO: These reads and writes should hit the same keys as other activities
	if err := a.rw.ReadN("Items", a.config.OrderItemsCnt); err != nil {
		return err
	}
	if err := a.rw.WriteN("StockKeepingUnits", a.config.OrderItemsCnt); err != nil {
		return err
	}
	return nil
}

func (a *AsyncDBSimulator) CompleteOrder() error {
	// TODO: In general, I should initialize access keys before I start the simulation for better lock integrity
	if err := a.rw.WriteN("Orders", 1); err != nil {
		return err
	}
	return nil
}

func NewAsyncDBSimulator(rw TableReadWriteSimulator, l *log.Logger, config *Config, keys int, bErrProb int) *AsyncDBSimulator {
	return &AsyncDBSimulator{
		rw:              rw,
		l:               l,
		config:          config,
		keys:            keys,
		businessErrProb: bErrProb,
	}
}
