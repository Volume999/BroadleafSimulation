package main

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/BroadleafSimulation/simulator"
	"github.com/Volume999/BroadleafSimulation/workflows"
	"github.com/Volume999/BroadleafSimulation/workload"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var config = simulator.RandomConfig()

func diskByType(diskType string, accessTimeMs int) workload.DiskAccessSimulator {
	switch diskType {
	case "unsafe":
		return workload.NewUnsafeDiskAccessSimulator(accessTimeMs)
	case "thread-safe":
		return workload.NewThreadSafeDiskAccessSimulator(accessTimeMs)
	default:
		panic("Invalid disk type")
	}
}

func simulatorByType(simulatorType string, config *simulator.Config, disk workload.DiskAccessSimulator) simulator.Simulator {
	switch simulatorType {
	case "sequential":
		return simulator.NewSequentialSimulator(config, disk)
	case "async":
		return simulator.NewAsyncSimulator(config, disk)
	default:
		panic("Invalid simulator type")
	}
}

func workflowByType(workflowType string, simulator simulator.Simulator) workflows.Workflow {
	switch workflowType {
	case "sequential":
		return workflows.NewSequentialWorkflow(simulator)
	case "async":
		return workflows.NewAsyncWorkflow(simulator)
	default:
		panic("Invalid workflow type")
	}
}

func BenchmarkSimulatedWorkflows(b *testing.B) {
	disks := []string{"thread-safe"}
	simulators := []string{"sequential", "async"}
	//simulators := []string{"sequential"}
	workflowTypes := []string{"sequential", "async"}
	//workflowTypes := []string{"sequential"}
	//parallelisms := []int{1, 10, 100, 1000, 2500, 5000, 10000, 20000, 40000, 80000, 120000}
	parallelisms := []int{1, 10, 100, 1000, 2500, 5000, 10000, 20000}
	//parallelisms := []int{1, 1000, 10000, 100000}
	limitConnections := []int{0, 200, 5000, 36000}
	//limitConnections := []int{0, 200}
	//limitConnections := []int{0}
	lockCount := []int{0, 100, 10000}
	//lockCount := []int{100}
	diskAccessTimesMs := []int{2, 10, 40, 100}
	//diskAccessTimesMs := []int{70, 100}
	//diskAccessTimesMs := []int{15}
	for _, limitConnectionsT := range limitConnections {
		for _, lockCountT := range lockCount {
			for _, diskT := range disks {
				for _, diskAccessTime := range diskAccessTimesMs {
					for _, simulatorT := range simulators {
						for _, workflowT := range workflowTypes {
							for _, parallelismT := range parallelisms {
								b.Run("disk="+diskT+"/accessTime(ms)="+strconv.Itoa(diskAccessTime)+"/simulator="+simulatorT+"/workflow="+workflowT+"/parallelism="+strconv.Itoa(parallelismT*runtime.NumCPU())+"/limitConnections="+strconv.Itoa(limitConnectionsT)+"/lockCount="+strconv.Itoa(lockCountT), func(b *testing.B) {
									b.SetParallelism(parallelismT)
									disk := diskByType(diskT, diskAccessTime)
									sim := simulatorByType(simulatorT, config, disk)
									if lockCountT > 0 {
										sim = simulator.NewWithContention(sim, lockCountT)
									}
									workflow := workflowByType(workflowT, sim)
									if limitConnectionsT > 0 {
										workflow = workflows.NewLimitedConnectionsWorkflow(workflow, limitConnectionsT)
									}
									benchStart := time.Now()
									totalFunctionTime := int64(0)
									b.ResetTimer()
									b.RunParallel(func(pb *testing.PB) {
										for pb.Next() {
											fnStart := time.Now()
											workflow.Execute()
											atomic.AddInt64(&totalFunctionTime, time.Since(fnStart).Milliseconds())
										}
									})
									// Avg Exec Time and Time Per Individual Function
									b.ReportMetric(0, "ns/op")
									b.ReportMetric(float64(time.Since(benchStart).Milliseconds())/float64(b.N), "ms/op1")
									b.ReportMetric(float64(totalFunctionTime)/float64(b.N), "ms/op2")
								})
							}
						}
					}
				}
			}
		}
	}
}

func getConfigCombinations(configs ...[]interface{}) [][]interface{} {
	if len(configs) == 0 {
		return [][]interface{}{}
	}
	if len(configs) == 1 {
		res := make([][]interface{}, 0)
		for _, c := range configs[0] {
			res = append(res, []interface{}{c})
		}
		return res
	}
	config := configs[0]
	otherConfigs := getConfigCombinations(configs[1:]...)
	res := make([][]interface{}, 0)
	for _, c := range config {
		for _, oc := range otherConfigs {
			res = append(res, append([]interface{}{c}, oc...))
		}
	}
	return res
}
func BenchmarkAsyncDBWorkflow(b *testing.B) {
	// TODO: Pg vs InMemory Simulation
	//keys := []interface{}{100, 1000, 10000}
	wfTypes := []interface{}{workflows.Sequential, workflows.Concurrent}
	simTypes := []interface{}{workflows.Sequential, workflows.Concurrent}
	parallelisms := []interface{}{1, 10, 100, 1000, 10000}
	f, err := os.OpenFile("simulation_bench.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic("Failed to open file: " + err.Error())
	}
	l := log.New(f, "AsyncDB Workflow: ", log.LstdFlags)
	runInstance := func(b *testing.B, instanceSetup func(db *asyncdb.AsyncDB, keys int) error, keys int, wfType string, simType string, parallelism int, businessErrProb int) {
		b.Run("keys="+strconv.Itoa(keys)+"/wfType="+wfType+"/simType="+simType+"/parallelism="+strconv.Itoa(parallelism), func(b *testing.B) {
			lm := asyncdb.NewLockManager()
			tm := asyncdb.NewTransactionManager()
			h := asyncdb.NewStringHasher()
			db := asyncdb.NewAsyncDB(tm, lm, h, asyncdb.WithExplicitTxn())
			if err := instanceSetup(db, keys); err != nil {
				panic("Failed to setup AsyncDB workflow: " + err.Error())
			}
			b.ResetTimer()
			benchStart := time.Now()
			totalFunctionTime := int64(0)
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				workflow := workflows.NewAsyncDBWorkflow(db, l, wfType, keys, businessErrProb)
				for pb.Next() {
					fnStart := time.Now()
					workflow.Execute(simType)
					atomic.AddInt64(&totalFunctionTime, time.Since(fnStart).Milliseconds())
				}
			})
			b.ReportMetric(0, "ns/op")
			b.ReportMetric(float64(time.Since(benchStart).Milliseconds())/float64(b.N), "ms/op1")
			b.ReportMetric(float64(totalFunctionTime)/float64(b.N), "ms/op2")
		})
	}
	//runBench := func(b *testing.B, configCombinations [][]interface{}, instanceSetup func(db *asyncdb.AsyncDB, keys int) error) {
	//	for _, config := range configCombinations {
	//		keys := config[0].(int)
	//		wfType := config[1].(string)
	//		simType := config[2].(string)
	//		parallelism := config[3].(int)
	//		businessErrProb := 0
	//		runInstance(b, instanceSetup, keys, wfType, simType, parallelism, businessErrProb)
	//	}
	//}
	//b.Run("TableType=InMemory", func(b *testing.B) {
	//	setup := func(db *asyncdb.AsyncDB, keys int) error {
	//		return workflows.SetupAsyncDBInMemoryWorkflow(db, keys)
	//	}
	//	configCombinations := getConfigCombinations(keys, wfTypes, simTypes, parallelisms)
	//	runBench(b, configCombinations, setup)
	//})
	//
	//b.Run("TableType=Postgres", func(b *testing.B) {
	//	connString := "postgres://postgres:secret@localhost:5432/postgres"
	//	if err := workflows.SetupPgTables(connString, 100000); err != nil {
	//		panic("Failed to set up Pg tables: " + err.Error())
	//	}
	//	pgFactory, err := asyncdb.NewPgTableFactory(connString)
	//	if err != nil {
	//		panic("Failed to create PgTableFactory: " + err.Error())
	//	}
	//	setup := func(db *asyncdb.AsyncDB, _ int) error {
	//		return workflows.SetupAsyncDBWorkflow(db, pgFactory)
	//	}
	//	configCombinations := getConfigCombinations(keys, wfTypes, simTypes, parallelisms)
	//	runBench(b, configCombinations, setup)
	//})

	b.Run("TableType=Simulated", func(b *testing.B) {
		setup := func(db *asyncdb.AsyncDB, keys int) error {
			return workflows.SetupAsyncDBSimulatedTablesWorkflow(db, keys)
		}
		configCombinations := getConfigCombinations(wfTypes, simTypes, parallelisms)
		for _, config := range configCombinations {
			wfType, simType, parallelism := config[0].(string), config[1].(string), config[2].(int)
			runInstance(b, setup, -1, wfType, simType, parallelism, 0)
		}
	})
}

//func BenchmarkDummy(b *testing.B) {
//	//parallelisms := []int{1, 10, 100, 1000, 10000, 100000}
//	b.SetParallelism(1)
//	i := 0
//	//b.SetParallelism(parallelism)
//	//now := time.Now()
//	count := int64(0)
//	totalTime := int64(0)
//	b.ResetTimer()
//	b.RunParallel(func(pb *testing.PB) {
//		atomic.AddInt64(&count, 1)
//		for pb.Next() {
//			now := time.Now()
//			util.SimulateSyncIoLoad()
//			atomic.AddInt64(&totalTime, int64(time.Since(now).Milliseconds()))
//		}
//	})
//	//b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(b.N), "ns/op.2")
//	b.ReportMetric(float64(i), "i")
//	b.ReportMetric(float64(count), "count")
//	b.ReportMetric(float64(totalTime)/float64(b.N), "totalTime(ms)")
//}
