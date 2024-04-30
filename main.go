package main

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/workflows"
	"sync"
)

func debugAsyncDBWorkflow() {
	lm := asyncdb.NewLockManager()
	tm := asyncdb.NewTransactionManager()
	h := asyncdb.NewStringHasher()
	db := asyncdb.NewAsyncDB(tm, lm, h)
	connString := "postgres://postgres:secret@localhost:5432/postgres"
	pgFactory, err := asyncdb.NewPgTableFactory(connString)
	if err != nil {
		panic("Failed to create PgTableFactory: " + err.Error())
	}
	table, err := pgFactory.GetTable("Orders")
	if err != nil {
		panic("Failed to get table: " + err.Error())
	}
	ctx, _ := db.Connect()
	if err = db.CreateTable(ctx, table); err != nil {
		panic("Failed to create table: " + err.Error())
	}
	if err := db.Disconnect(ctx); err != nil {
		panic("Failed to disconnect: " + err.Error())
	}
	wg := sync.WaitGroup{}
	wg.Add(10)
	for range 10 {
		go func() {
			defer wg.Done()
			workflow := workflows.NewAsyncDBWorkflow(db, workflows.ConcurrentSimulationType, 100)
			workflow.Execute()
		}()
	}
	wg.Wait()
}

func main() {
	debugAsyncDBWorkflow()
}
