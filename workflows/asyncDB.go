package workflows

import (
	"errors"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/simulator"
)

const (
	ConcurrentSimulationType = "concurrent"
	SequentialSimulationType = "sequential"
)

type AsyncDBWorkflow struct {
	db  *asyncdb.AsyncDB
	ctx *asyncdb.ConnectionContext
	s   *simulator.AsyncDBSimulator
}

func NewAsyncDBWorkflow(db *asyncdb.AsyncDB, simType string, keys int) *AsyncDBWorkflow {
	// Connect
	ctx, _ := db.Connect()

	var tableRWSimulator simulator.TableReadWriteSimulator
	switch simType {
	case "concurrent":
		tableRWSimulator = simulator.NewConcTableReadWriteSimulator(db, ctx, keys)
	case "sequential":
		tableRWSimulator = simulator.NewSyncTableReadWriteSimulator(db, ctx, keys)
	}

	config := simulator.RandomConfig()
	sim := simulator.NewAsyncDBSimulator(tableRWSimulator, config, keys)
	return &AsyncDBWorkflow{db, ctx, sim}
}

func SetupAsyncDBWorkflow(db *asyncdb.AsyncDB, pgFactory *asyncdb.PgTableFactory, keys int) error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "TaxProviders", "OrderTaxes"}
	ctx, _ := db.Connect()
	for _, table := range tables {
		tbl, err := pgFactory.GetTable(table)
		if err != nil {
			return err
		}
		for i := 0; i <= keys; i++ {
			err = tbl.Put(i, "value")
			if err != nil {
				return err
			}
		}
		err = db.CreateTable(ctx, tbl)
		if err != nil {
			return err
		}
	}
	err := db.Disconnect(ctx)
	return err
}

func (w *AsyncDBWorkflow) withTransaction(workflow func() error) {
	err := w.db.BeginTransaction(w.ctx)
	if err != nil {
		panic("Failed to begin transaction: " + err.Error())
	}
	err = workflow()
	for err != nil {
		if errors.Is(err, simulator.ErrBusinessLogic) {
			err = w.db.RollbackTransaction(w.ctx)
			if err != nil {
				panic("Failed to rollback transaction: " + err.Error())
			}
			return
		} else {
			err = workflow()
		}
	}
	err = w.db.CommitTransaction(w.ctx)
	if err != nil {
		panic("Failed to commit transaction: " + err.Error())
	}
}

func (w *AsyncDBWorkflow) ExecuteConcurrent() error {
	validationPhase := []func() error{
		w.s.ValidateCheckout,
		w.s.ValidateAvailability,
		w.s.VerifyCustomer,
		w.s.ValidatePayment,
	}

	errChan := make(chan error, len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func() error) {
			errChan <- activity()
		}(activity)
	}
	for i := 0; i < len(validationPhase); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	operationPhase := []func() error{
		w.s.RecordOffer,
		w.s.CommitTax,
		w.s.DecrementInventory,
	}
	errChan = make(chan error, len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func() error) {
			errChan <- activity()
		}(activity)
	}
	for i := 0; i < len(operationPhase); i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}
	if err := w.s.CompleteOrder(); err != nil {
		return err
	}
	return nil
}

func (w *AsyncDBWorkflow) Execute() {
	w.withTransaction(w.ExecuteConcurrent)
}
