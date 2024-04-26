package workload

import (
	"time"
)

func SimulateCpuLoad(cpuLoadCycles int) {
	for range cpuLoadCycles {
	}
}

func SimulateSyncIoLoad(timeMs int) {
	time.Sleep(time.Duration(timeMs) * time.Millisecond)
}
