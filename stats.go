package pelichan

import (
	"sync/atomic"
)

type diskBufferedChanStats struct {
	DirectPasses  uint64
	StorageWrites uint64
	StorageReads  uint64
}

func (stats *diskBufferedChanStats) IncDP() {
	safeInc(&stats.DirectPasses)
}

func (stats *diskBufferedChanStats) IncSW() {
	safeInc(&stats.StorageWrites)
}

func (stats *diskBufferedChanStats) IncSR() {
	safeInc(&stats.StorageReads)
}

func (stats *diskBufferedChanStats) GetStats() (directPasses uint64, storageWrites uint64, storageReads uint64) {
	return atomic.LoadUint64(&stats.DirectPasses),
		atomic.LoadUint64(&stats.StorageWrites),
		atomic.LoadUint64(&stats.StorageReads)
}

func safeInc(metric *uint64) {
	// Defer to catch and fix overflows
	defer func() {
		if r := recover(); r != nil {
			atomic.StoreUint64(metric, 0)
		}
	}()

	atomic.AddUint64(metric, 1)
}
