package main

import (
	"fmt"
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/workflows"
	"log"
	"os"
	"sync"
)

func debugAsyncDBWorkflow() {
	lm := asyncdb.NewLockManager()
	tm := asyncdb.NewTransactionManager()
	h := asyncdb.NewStringHasher()
	db := asyncdb.NewAsyncDB(tm, lm, h)
	f, err := os.OpenFile("simulation.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	connString := "postgres://postgres:secret@localhost:5432/postgres"
	pgFactory, err := asyncdb.NewPgTableFactory(connString)
	if err != nil {
		panic("Failed to create PgTableFactory: " + err.Error())
	}
	if err = workflows.SetupPgTables(connString, 100000); err != nil {
		panic("Failed to setup Pg tables: " + err.Error())
	}
	if err = workflows.SetupAsyncDBWorkflow(db, pgFactory); err != nil {
		panic("Failed to setup AsyncDB workflow: " + err.Error())
	}
	wg := sync.WaitGroup{}
	threads := 100
	iters := 2
	wg.Add(threads)
	for i := range threads {
		go func() {
			defer wg.Done()
			i := i
			l := log.New(f, fmt.Sprintf("Workflow #%v: ", i+1), log.LstdFlags)
			//l := log.New(os.Stdout, fmt.Sprintf("Workflow #%v: ", i+1), log.LstdFlags)
			workflow := workflows.NewAsyncDBWorkflow(db, l, workflows.ConcurrentSimulationType, 1, 0)
			for range iters {
				workflow.Execute(workflows.ConcurrentSimulationType)
			}
		}()
	}
	wg.Wait()
}

func main() {
	debugAsyncDBWorkflow()
}
