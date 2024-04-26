package workload

import (
	"sync"
)

type DiskAccessSimulator interface {
	SimulateDiskAccess()
}

type UnsafeDiskAccessSimulator struct {
	accessTimeMs int
}

func NewUnsafeDiskAccessSimulator(accessTimeMs int) *UnsafeDiskAccessSimulator {
	return &UnsafeDiskAccessSimulator{
		accessTimeMs: accessTimeMs,
	}
}

func (u *UnsafeDiskAccessSimulator) SimulateDiskAccess() {
	SimulateSyncIoLoad(u.accessTimeMs)
}

type ThreadSafeDiskAccessSimulator struct {
	lock         *sync.Mutex
	accessTimeMs int
}

func NewThreadSafeDiskAccessSimulator(accessTimeMs int) *ThreadSafeDiskAccessSimulator {
	return &ThreadSafeDiskAccessSimulator{
		lock:         &sync.Mutex{},
		accessTimeMs: accessTimeMs,
	}
}

func (t *ThreadSafeDiskAccessSimulator) SimulateDiskAccess() {
	SimulateSyncIoLoad(t.accessTimeMs)
	t.lock.Lock()
	// Writing to the log file
	SimulateCpuLoad(10)
	t.lock.Unlock()
}
