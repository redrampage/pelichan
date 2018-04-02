package pelichan

import (
	"github.com/beeker1121/goque"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TODO: DeqErrCB and DecErrCB tests

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	rand.Seed(time.Now().UnixNano())
}

type MyStr struct {
	Data string
}

type MyInt struct {
	Data int
}

func DecMyStr(item *goque.Item) (interface{}, error) {
	var obj *MyStr
	err := item.ToObject(&obj)
	return obj, err
}

func DecMyInt(item *goque.Item) (interface{}, error) {
	var obj *MyInt
	err := item.ToObject(&obj)
	return obj, err
}

const dbDirectory = "TMPDB"

func prepChan(t *testing.T, indepth, outdepth int,
	decCB func(item *goque.Item) (interface{}, error)) (
	dbch *DiskBufferedChan,
	src chan interface{},
	sink <-chan interface{},
	abort chan struct{}) {

	src = make(chan interface{}, indepth)

	dbch, sink, err := NewDiskBufferedChan(dbDirectory, outdepth, decCB, src, &testLogger{t})
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	return
}

func suck(t *testing.T, sink <-chan interface{}) (cnt int) {
	for range sink {
		cnt++
	}
	return
}

const iters = 10
const sleepTime = time.Millisecond * 50

func TestDiskBufferedChan_SimpleDirect(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyStr)

	go func() {
		for i := 0; i < iters; i++ {
			<-sink
		}
	}()

	for i := 0; i < iters; i++ {
		src <- &MyStr{"Data_" + strconv.Itoa(i)}
		time.Sleep(sleepTime)
	}

	fwd, wr, rd := d.GetStats()
	d.Close()

	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	if fwd != uint64(iters) {
		t.Fatalf("Non direct send detected!")
	}
	if wr > 0 || rd > 0 {
		t.Fatalf("Parasite reads/writes detected")
	}
}

func TestDiskBufferedChan_SimpleDirectPartial(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyStr)

	go func() {
		for i := 0; i < iters/2; i++ {
			<-sink
		}
	}()

	for i := 0; i < iters; i++ {
		src <- &MyStr{"Data_" + strconv.Itoa(i)}
		time.Sleep(sleepTime)
	}

	time.Sleep(sleepTime * 5)

	for i := 0; i < (iters - iters/2); i++ {
		<-sink
	}

	fwd, wr, rd := d.GetStats()
	d.Close()

	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	if fwd != uint64(iters/2) {
		t.Fatalf("Non direct sends are more than half!")
	}
	if wr != rd || wr != (iters-iters/2) {
		t.Fatalf("Parasite reads/writes detected")
	}
}

func TestDiskBufferedChan_SuddenAbort(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyStr)

	var sendBlockedOn = -1
	var sum1 int
	go func() {
		sum1 = suck(t, sink)
	}()

	var abortNum = (rand.Int() % (iters - 1)) + 1
OUT:
	for i := 1; i <= iters; i++ {
		select {
		case src <- &MyStr{"Data_" + strconv.Itoa(i)}:
		case <-time.Tick(time.Second):
			t.Logf("Send timeout abort on '%d'", i)
			sendBlockedOn = i
			break OUT
		}

		if i == abortNum {
			t.Logf("Sudden abort on %d", i)
			d.Halt()
			//close(d.chHalt) // Hacky abort without wait for completion
			//<-d.chFWDone    // ...well, with a bit, to ensure precise timeout iteration
		}
	}

	fwd, wr, rd := d.GetStats()
	close(src)
	d.Close()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	t.Logf("Received: %d\n", sum1)

	if sendBlockedOn != abortNum+1 {
		t.Fatalf("Source send timeout on wrong item, expected '%d', got '%d'", abortNum+1, sendBlockedOn)
	}

	// Get leftover records
	d2, src, sink2, _ := prepChan(t, 0, 0, DecMyStr)
	close(src)

	sum2 := suck(t, sink2)
	d2.Close()

	fwd, wr, rd = d2.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	t.Logf("Received: %d\n", sum2)
	if (sum1 + sum2) != abortNum {
		t.Fatalf("Record number mismatch: %d/%d", sum1+sum2, abortNum+1)
	}
}

func TestDiskBufferedChan_StuckSink(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, _, _ := prepChan(t, 0, 0, DecMyStr)

OUT:
	for i := 0; i < iters; i++ {
		select {
		case src <- &MyStr{"Data_" + strconv.Itoa(i)}:
		case <-time.Tick(time.Second):
			t.Logf("Send timeout abort on '%d'", i)
			break OUT
		}
	}

	d.Close()

	fwd, wr, rd := d.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)

	if fwd > 0 {
		t.Fatalf("Impossible forwards on stuck sink, test broken")
	}

	// Now check what is on disk
	d2, src, sink2, _ := prepChan(t, 0, 0, DecMyStr)
	close(src)

	sum := suck(t, sink2)
	d2.Close()

	fwd, wr, rd = d2.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	if rd != uint64(iters) {
		t.Fatalf("Record stat number mismatch: %d/%d", rd, iters)
	}
	if sum != iters {
		t.Fatalf("Received number mismatch: %d/%d", sum, iters)
	}
}

func TestDiskBufferedChan_SourceClose(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyStr)
	defer d.Close()

	for i := 1; i <= iters; i++ {
		src <- &MyStr{"Data_" + strconv.Itoa(i)}
	}
	close(src)

	fwd, wr, rd := d.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)

	gotnum := suck(t, sink)
	t.Logf("Got %d/%d", gotnum, iters)
	if gotnum != iters {
		t.Fatalf("Expected %d items, got %d", iters, gotnum)
	}
}

func TestDiskBufferedChan_SourceCloseAndHalt(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyStr)

	for i := 1; i <= iters; i++ {
		src <- &MyStr{"Data_" + strconv.Itoa(i)}
	}
	close(src)

	fwd, wr, rd := d.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)

	var abortNum = (rand.Int() % (iters - 1)) + 1
	var cnt = 0
	for range sink {
		cnt++
		if abortNum == cnt {
			d.HaltAsync()
		}
	}
	t.Logf("Got %d/%d, aborted on %d", cnt, iters, abortNum)
	d.WaitHalt()
	d.Close()

	d, src, sink, _ = prepChan(t, 0, 0, DecMyStr)
	defer d.Close()
	close(src)
	for range sink {
		cnt++
	}

	t.Logf("Got %d/%d", cnt, iters)
	if cnt != iters {
		t.Fatalf("Expected %d items, got %d", iters, cnt)
	}
}

func TestDiskBufferedChan_SourceCloseBufferedSink(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, iters, DecMyStr)
	defer d.Close()

	for i := 1; i <= iters; i++ {
		src <- &MyStr{"Data_" + strconv.Itoa(i)}
	}
	close(src)

	fwd, wr, rd := d.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)

	time.Sleep(sleepTime*3)
	t.Logf("Disk has '%d' records", d.ldbQueue.Length())
	if d.ldbQueue.Length() > 0 {
		t.Fatalf("'%d' messages got back-sucked after src close", d.ldbQueue.Length())
	}

	gotnum := suck(t, sink)
	t.Logf("Got %d/%d", gotnum, iters)
	if gotnum != iters {
		t.Fatalf("Expected %d items, got %d", iters, gotnum)
	}
}

func TestDiskBufferedChan_Consistency(t *testing.T) {
	os.RemoveAll(dbDirectory)
	d, src, sink, _ := prepChan(t, 0, 0, DecMyInt)
	defer d.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	var sum_src = 0
	go func() {
		defer wg.Done()
		for i := 1; i <= iters; i++ {
			num := int(rand.Int31())
			src <- &MyInt{num}
			sum_src += num
		}
	}()

	var sum_sink = 0
	go func() {
		defer wg.Done()
		for i := 1; i <= iters; i++ {
			data := <-sink
			sum_sink += data.(*MyInt).Data
		}
	}()

	wg.Wait()
	fwd, wr, rd := d.GetStats()
	t.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)

	t.Logf("Summs: %d/%d", sum_src, sum_sink)
	if sum_src != sum_sink {
		t.Fatalf("Summs mismatch: %d/%d", sum_src, sum_sink)
	}
}

func BenchmarkDiskBufferedChan_FWD(b *testing.B) {
	os.RemoveAll(dbDirectory)
	src := make(chan interface{})

	dbch, sink, err := NewDiskBufferedChan(dbDirectory, 0, DecMyStr, src)
	if err != nil {
		b.Fatalf("Error: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src <- &MyStr{strconv.Itoa(i)}
		<-sink
	}
	b.StopTimer()

	fwd, wr, rd := dbch.GetStats()
	b.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	dbch.Close()
}

func BenchmarkDiskBufferedChan_DiskRnW(b *testing.B) {
	os.RemoveAll(dbDirectory)
	src := make(chan interface{})

	dbch, sink, err := NewDiskBufferedChan(dbDirectory, 0, DecMyStr, src)
	if err != nil {
		b.Fatalf("Error: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src <- &MyStr{strconv.Itoa(i)}
	}
	close(src)
	for i := 0; i < b.N; i++ {
		<-sink
	}
	b.StopTimer()

	fwd, wr, rd := dbch.GetStats()
	b.Logf("Pipe stats:\nDirect: %d\nWrites: %d\nReads: %d\n", fwd, wr, rd)
	dbch.Close()
}

type testLogger struct {
	testing *testing.T
}

func (l *testLogger) Debug(args ...interface{}) {
	l.testing.Log(args...)
}
func (l *testLogger) Debugf(format string, args ...interface{}) {
	l.testing.Logf(format, args...)
}

func (l *testLogger) Info(args ...interface{}) {
	l.testing.Log(args...)
}
func (l *testLogger) Infof(format string, args ...interface{}) {
	l.testing.Logf(format, args...)
}

func (l *testLogger) Warn(args ...interface{}) {
	l.testing.Log(args...)
}
func (l *testLogger) Warnf(format string, args ...interface{}) {
	l.testing.Logf(format, args...)
}

func (l *testLogger) Error(args ...interface{}) {
	l.testing.Error(args...)
}
func (l *testLogger) Errorf(format string, args ...interface{}) {
	l.testing.Errorf(format, args...)
}
