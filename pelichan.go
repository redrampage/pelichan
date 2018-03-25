package pelichan

import (
	"github.com/beeker1121/goque"
	"sync"
	"time"
)

type DiskBufferedChan struct {
	// LevelDB storage path
	ldbPath string

	// LevelDB queue instance
	ldbQueue *goque.Queue

	// `Source` channel
	// All incoming data comes from here
	// User is responsible to save any data buffered in here
	source <-chan interface{}

	// `Sink` channel
	// All outgoing data sent here
	// On abort, will try to save all data buffered here on disk
	sink chan interface{}

	////// Signalling internals below //////

	// `Sink` Waitgroup
	// Waits stop of `DiskReader` and `SourceForwarder` routines
	sinkWg sync.WaitGroup

	// Abort signal channel
	// When closed signals `SourceForwarder` and `DiskReader` to cease reading/forwarding
	chAbort chan struct{}

	// Internal `DiskReader` notification channel
	// Not to be closed! TODO: check if it's leaky without closing
	// Signals `DiskReader`, that new data is available to read from disk
	chDRNotify chan struct{}

	// Shutdown Done signal channel
	// Notifies that both `SourceForwarder` and `DiskReader` routines has stopped (just as sinkWg but...),
	// also that `Sink` is clear of any buffered records (either sucked back on disk, or taken by anything reading sink)
	// and will be closed immediately
	shutdownDone chan struct{}

	// LevelDB Done signal channel
	// Notifies that shutdown of everything is complete and LevelDB handler has been closed
	ldbDone chan struct{}

	// `DiskReader` signal channels
	// `Abort` initiates immediate `DiskReader` shutdown sequence, leaving data on disk
	// `FlushAbort` initiates `DiskReader` abort after all current data onk disk has been dumped into `Sink`
	// `Done` signals that `DiskReader` has finished it's activity
	chDRAbort      chan struct{}
	chDRFlushAbort chan struct{}
	chDRDone       chan struct{}

	// `SourceForwarder` signal channels
	// `Abort` initiates `SourceForwarder` shutdown sequence
	// `Done` signals that `SourceForwarder` has finished
	chFWAbort chan struct{}
	chFWDone  chan struct{}

	////// Statistics //////

	// Contain counters of `DiskBufferedChan` activity
	stats  DiskBufferedChanStats
	logger genericLogger

	////// Le Generiq de Golangaux™ below //////

	// Decoding Callback
	// This is a user supplied function that should map goque item read from disk to object of concrete type used
	decCB func(item *goque.Item) (interface{}, error)

	// This is optional callback function.
	// Called in case of failure to store incoming object on disk.
	EnqErrCB func(obj interface{}, err error)

	// This is optional callback function.
	// Called in case of failure to retrieve object from disk.
	// If it returns `true` then reading from disk is stopped and abort channel is closed
	DeqErrCB func(err error) (abort bool)

	// This is optional callback function.
	// Called in case of failure to decode object retrieved from disk.
	// This callback can try to recover by returning object of type that should've been decoded.
	// If it return nil, then object sending is skipped.
	// If it returns abort param as `true` then reading from disk is stopped and abort channel is closed
	DecErrCB func(item *goque.Item, err error) (obj interface{}, abort bool)
}

func NewDiskBufferedChan(
	ldbPath string,
	sinkDepth int,
	decCB func(item *goque.Item) (interface{}, error),
	src <-chan interface{},
	logger ...genericLogger,
) (ret *DiskBufferedChan, sink <-chan interface{}, err error) {

	if decCB == nil {
		panic("Cannot create DiskBufferedChan with nil decode callback")
	}

	var log genericLogger
	switch len(logger) {
	case 0:
		// No logger given, use VoidLogger
		log = &voidLogger{}
	case 1:
		// We got logger to use
		log = logger[0]
	default:
		panic("Only one logger is supported")
	}

	ret = &DiskBufferedChan{
		ldbPath:        ldbPath,
		source:         src,
		logger:         log,
		sink:           make(chan interface{}, sinkDepth),
		chAbort:        make(chan struct{}),
		shutdownDone:   make(chan struct{}),
		ldbDone:        make(chan struct{}),
		chDRAbort:      make(chan struct{}),
		chDRFlushAbort: make(chan struct{}),
		chDRDone:       make(chan struct{}),
		chFWAbort:      make(chan struct{}),
		chFWDone:       make(chan struct{}),
		chDRNotify:     make(chan struct{}, 1),
		decCB:          decCB,
		stats:          DiskBufferedChanStats{},
	}

	// Open LevelDB queue
	ret.ldbQueue, err = goque.OpenQueue(ldbPath)
	if err != nil {
		return nil, nil, err
	}

	ret.startWriters()
	return ret, ret.sink, nil
}

// Returns statistics of current instance
//
// `directPasses` is a number of object passed from `Source` to `Sink` without storing on disk
// `storageWrites` is a number of objects that has been stored to disk
// `storageReads` is a number of objects that has been read from disk
//
// Direct passes is a lot quick relative to latter two, so you should try to minimize number of disk ops
// first thing to do, would be increasing `sinkDepth`
func (c *DiskBufferedChan) GetStats() (directPasses uint64, storageWrites uint64, storageReads uint64) {
	return c.stats.GetStats()
}

// Wait till complete shutdown of instance
func (c *DiskBufferedChan) Wait() {
	<-c.ldbDone
}

// Abort current instance and wait till complete shutdown of it
func (c *DiskBufferedChan) Abort() {
	// Check if abort is already signalled
	select {
	case <-c.chAbort:
		return
	default:
		close(c.chAbort)
	}
	c.Wait()
}

// Stores given object in PersistentDiskQueue
func (c *DiskBufferedChan) store(obj interface{}) {
	c.stats.IncSW()
	_, err := c.ldbQueue.EnqueueObject(obj)
	if err != nil {
		if c.EnqErrCB != nil {
			c.logger.Errorf("Failed to store object '%+v': %s", obj, err)
			c.EnqErrCB(obj, err)
		}
	}
}

// Retrieve next stored object from PersistentDiskQueue
func (c *DiskBufferedChan) retrieve() (obj interface{}, abort bool) {
	c.stats.IncSR()

	item, err := c.ldbQueue.Dequeue()
	if err != nil {
		if c.DeqErrCB != nil {
			abort = c.DeqErrCB(err)
		}
		//return nil, abort
		return
	}

	obj, err = c.decCB(item)
	if err != nil {
		if c.DecErrCB != nil {
			obj, abort = c.DecErrCB(item, err)
		}
	}
	//return obj, abort
	return
}

func (c *DiskBufferedChan) sendOrStore(obj interface{}) {
	select {
	case c.sink <- obj:
		// Direct send
		c.stats.IncDP()
	default:
		// Sink is full, save obj on disk
		c.store(obj)
		// Saved object, kick reader just in case
		c.notifyReader()
	}
}

// Writer routine
//
// Tries to pass incoming data from source directly to sink
// If sink is currently cannot receive data, startWriters will
// try to write it to persistent storage (this is blocking)
func (c *DiskBufferedChan) startWriters() {

	// Abort listener
	go func() {
		<-c.chAbort
		close(c.chDRAbort)
		close(c.chFWAbort)
	}()

	// LevelDB close listener
	go func() {
		defer close(c.ldbDone)
		<-c.shutdownDone
		err := c.ldbQueue.Close()
		if err != nil {
			c.logger.Errorf("Failed to close LevelDB: %s", err)
		}
	}()

	// Src close listener
	go func() {
		select {
		case <-c.chFWDone:
			close(c.chDRFlushAbort)
		case <-c.chDRAbort:
		}
	}()

	// Add for sendOrStore thread
	// Add for diskQueueReader thread
	// Both routines will push data in sink, so we must close sink strictly
	// after them both shutdownDone
	c.sinkWg.Add(2)

	// SendOrStore thread
	go func(c *DiskBufferedChan) {
		defer c.sinkWg.Done()
		defer close(c.chFWDone)
		defer c.logger.Debugf("Forwarder thread done")
		for {
			// Early abort
			select {
			case <-c.chFWAbort:
				// Stop forwarding messages on abort
				return
			default:
			}

			select {
			case <-c.chFWAbort:
				// Stop forwarding messages on abort
				return
			case obj, ok := <-c.source:
				// Exit when source closes
				if !ok {
					return
				}
				c.sendOrStore(obj)
			}
		}
	}(c)

	// DiskReader thread
	// It blocks with one item, util sink is clear to send
	// We must retrieve and re-store this item on abort
	go func(c *DiskBufferedChan) {
		defer c.sinkWg.Done()
		defer c.logger.Debugf("Disk reader thread done")
		discCh := c.getDiskReader()

		for obj := range discCh {
			// Early abort catch
			select {
			case <-c.chDRAbort:
				// Re-store received blocked object on abort
				c.logger.Debugf("Got item '%s' from disk reader, but abort signalled, so storing", obj)
				c.store(obj)
				continue
			default:
			}
			// Store object on abort when blocked on send to sink
			select {
			case c.sink <- obj:
			case <-c.chDRAbort:
				// Re-store received blocked object on abort
				c.logger.Debugf("Got item '%s' pushing to sink, but abort signalled, so storing", obj)
				c.store(obj)
			}
		}
	}(c)

	// Sink closer thread
	// It closes the sink channel when it's feeders are stopped
	// Also it stores all leftover items in sink
	go func(c *DiskBufferedChan) {
		defer close(c.sink)
		defer close(c.shutdownDone)

		// Wait for abort signal
		c.sinkWg.Wait()

		c.logger.Debugf("Both feeders done, time to flush sink")
		// Feeders closed, suck up remaining items to persistent storage
		c.logger.Debugf("START: sink len = %d", len(c.sink))
		// Suck-back sink, only on abort, else wait for sink clearup
		select {
		case <-c.chDRAbort:
			for len(c.sink) > 0 {
				c.logger.Debugf("LOOP: sink len = %d", len(c.sink))
				select {
				case item := <-c.sink:
					c.store(item)
				default:
				}
			}
		default:
			// FIXME: abort after SRC close may result in failure here
			// FIXME: sleep is hacky, need something better
			for len(c.sink) > 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(c)
}

// Routine that injects stored items from LevelDB to sink
func (c *DiskBufferedChan) getDiskReader() <-chan interface{} {
	diskCh := make(chan interface{})

	go func(diskCh chan interface{}) {
		defer c.logger.Debugf("Exited disk reader")
		defer close(c.chDRDone)
		defer close(diskCh)

		var abortOnNext = false
		for {
			if c.ldbQueue.Length() == 0 {
				// No new data, sleep till arrival
				select {
				case <-c.chDRAbort:
					// Got abort event, should cease reading
					c.logger.Debugf("DR got abort in wait")
					return
				case <-c.chDRFlushAbort:
					// Got lazy abort event, should cease reading on next pass
					c.logger.Debugf("DR got lazy abort in wait")
					if abortOnNext {
						return
					}
					abortOnNext = true
				case <-c.chDRNotify:
					// Just proceed
				}
			}

			for c.ldbQueue.Length() > 0 {
				select {
				case <-c.chDRAbort:
					// Got abort event, should cease reading
					c.logger.Debugf("DR got abort in retrieve")
					return
				default:
					obj, abort := c.retrieve()
					if abort {
						// Got abort decision from user func
						close(c.chAbort) // Global abort
						return
					}
					if obj != nil {
						diskCh <- obj
					}
				}
			}
		}
	}(diskCh)
	return diskCh
}

// Non-blocking notification of reader routine about new data in LevelDB
func (c *DiskBufferedChan) notifyReader() {
	select {
	case c.chDRNotify <- struct{}{}:
	default:
	}
}
