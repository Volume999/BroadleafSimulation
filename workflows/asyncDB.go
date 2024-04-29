package workflows

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/simulator"
)

type AsyncDBWorkflow struct {
	pgFactory *asyncdb.PgTableFactory
	db        *asyncdb.AsyncDB
	s         simulator.AsyncDBSimulator
	keys      int
}

func NewAsyncDBWorkflow(pg *asyncdb.PgTableFactory, db *asyncdb.AsyncDB, s simulator.AsyncDBSimulator, k int) *AsyncDBWorkflow {
	return &AsyncDBWorkflow{pg, db, s, k}
}

func (w *AsyncDBWorkflow) Setup() error {
	tables := []string{"Orders", "Items", "StockKeepingUnits", "Customers", "ItemOffers", "OrderPayments", "ItemOptions", "CustomerOffersUsage", "TaxProviders", "OrderTaxes"}
	ctx, _ := w.db.Connect()
	for _, table := range tables {
		tbl, err := w.pgFactory.GetTable(table)
		if err != nil {
			return err
		}
		for i := 0; i < w.keys; i++ {
			err = tbl.Put(i, "value")
			if err != nil {
				return err
			}
		}
		err = w.db.CreateTable(ctx, tbl)
		if err != nil {
			return err
		}
	}
	err := w.db.Disconnect(ctx)
	return err
}

func (w *AsyncDBWorkflow) Execute() {

}
