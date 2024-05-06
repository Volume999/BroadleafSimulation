package workflows

import (
	"context"
	"errors"
	"fmt"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/simulator"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"time"
)

const (
	ConcurrentSimulationType = "concurrent"
	SequentialSimulationType = "sequential"
)

type AsyncDBWorkflow struct {
	db      *asyncdb.AsyncDB
	l       *log.Logger
	ctx     *asyncdb.ConnectionContext
	s       *simulator.AsyncDBSimulator
	simType string
	keys    int
}

func NewAsyncDBWorkflow(db *asyncdb.AsyncDB, l *log.Logger, simType string, keys int, bErrProb int) *AsyncDBWorkflow {
	// Connect
	l.Println("Initializing Workflow")
	ctx, _ := db.Connect()

	var tableRWSimulator simulator.TableReadWriteSimulator
	switch simType {
	case "concurrent":
		tableRWSimulator = simulator.NewConcTableReadWriteSimulator(db, ctx, keys)
	case "sequential":
		tableRWSimulator = simulator.NewSyncTableReadWriteSimulator(db, ctx, keys)
	}

	config := simulator.RandomConfig()
	sim := simulator.NewAsyncDBSimulator(tableRWSimulator, l, config, keys, bErrProb)
	return &AsyncDBWorkflow{db, l, ctx, sim, simType, keys}
}

func SetupAsyncDBInMemoryWorkflow(db *asyncdb.AsyncDB, keys int) error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "OrderTaxes"}
	ctx, _ := db.Connect()

	for _, table := range tables {
		tbl, err := asyncdb.NewInMemoryTable[int, string](table)
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

func SetupPgTables(connString string, keys int) error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "OrderTaxes"}
	pgctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(pgctx, connString)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if _, err = pool.Exec(pgctx, fmt.Sprintf("INSERT INTO %s (key, value) SELECT k, 'value' FROM generate_series(1, %v) k ON CONFLICT (key) DO NOTHING", table, keys)); err != nil {
			return err
		}
	}
	pool.Close()
	return nil
}

func SetupAsyncDBWorkflow(db *asyncdb.AsyncDB, pgFactory *asyncdb.PgTableFactory) error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "TaxProviders", "OrderTaxes"}
	ctx, _ := db.Connect()
	for _, table := range tables {
		tbl, err := pgFactory.GetTable(table)
		if err != nil {
			return err
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
	w.l.Println("Starting Transaction")
	err := w.db.BeginTransaction(w.ctx)
	if err != nil {
		panic("Failed to begin transaction: " + err.Error())
	}
	ts := w.ctx.Txn.Timestamp()
	err = workflow()
	for err != nil {
		w.l.Println("Workflow failed with error: ", err.Error())
		if errors.Is(err, simulator.ErrBusinessLogic) {
			err = w.db.RollbackTransaction(w.ctx)
			w.l.Println("Transaction is rolled back: Business error")
			if err != nil {
				panic("Failed to rollback transaction: " + err.Error())
			}
			return
		} else {
			w.l.Println("Transaction is aborted: Retrying")
			// You should just retry the workflow, but because concurrent executions can take over new transaction,
			// I disconnect and connect again and start over
			//err = workflow()
			if rollBackErr := w.db.RollbackTransaction(w.ctx); rollBackErr != nil {
				panic("Failed to rollback transaction: " + rollBackErr.Error())
			}
			//_ = w.db.Disconnect(w.ctx)
			w.ctx, _ = w.db.Connect()
			if err = w.db.BeginTransaction(w.ctx); err != nil {
				panic("Failed to begin transaction: " + err.Error())
			}
			w.ctx.Txn.SetTimestamp(ts)
			//w.s.SetConnCtx(w.ctx)

			var tableRWSimulator simulator.TableReadWriteSimulator
			switch w.simType {
			case "concurrent":
				tableRWSimulator = simulator.NewConcTableReadWriteSimulator(w.db, w.ctx, w.keys)
			case "sequential":
				tableRWSimulator = simulator.NewSyncTableReadWriteSimulator(w.db, w.ctx, w.keys)
			}

			config := simulator.RandomConfig()
			w.s = simulator.NewAsyncDBSimulator(tableRWSimulator, w.l, config, w.keys, 0)
			err = workflow()
		}
	}
	err = w.db.CommitTransaction(w.ctx)
	if err != nil {
		panic("Failed to commit transaction: " + err.Error())
	}
	w.l.Println("Transaction is committed")
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
	var valErr error
	for i := 0; i < len(validationPhase); i++ {
		if err := <-errChan; err != nil {
			valErr = errors.Join(valErr, err)
		}
	}
	if valErr != nil {
		return valErr
	}

	operationPhase := []func() error{
		w.s.RecordOffer,
		w.s.CommitTax,
		w.s.DecrementInventory,
	}
	var opErr error
	errChan = make(chan error, len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func() error) {
			errChan <- activity()
		}(activity)
	}
	for i := 0; i < len(operationPhase); i++ {
		if err := <-errChan; err != nil {
			opErr = errors.Join(opErr, err)
		}
	}
	if opErr != nil {
		return opErr
	}
	if err := w.s.CompleteOrder(); err != nil {
		return err
	}
	return nil
}

func (w *AsyncDBWorkflow) ExecuteSequential() error {
	if err := w.s.ValidateCheckout(); err != nil {
		return err
	}
	if err := w.s.ValidateAvailability(); err != nil {
		return err
	}
	if err := w.s.VerifyCustomer(); err != nil {
		return err
	}
	if err := w.s.ValidatePayment(); err != nil {
		return err
	}
	if err := w.s.RecordOffer(); err != nil {
		return err
	}
	if err := w.s.CommitTax(); err != nil {
		return err
	}
	if err := w.s.DecrementInventory(); err != nil {
		return err
	}
	if err := w.s.CompleteOrder(); err != nil {
		return err
	}
	return nil
}

func (w *AsyncDBWorkflow) Execute(wfType string) {
	switch wfType {
	case ConcurrentSimulationType:
		w.withTransaction(w.ExecuteConcurrent)
	case SequentialSimulationType:
		w.withTransaction(w.ExecuteSequential)
	}
}
