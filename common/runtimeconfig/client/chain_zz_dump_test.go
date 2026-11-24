package client

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// Diagnostic only (Tessel Works fork CI): dump goroutines left behind by earlier tests.
func TestZZDump(t *testing.T) {
	time.Sleep(300 * time.Millisecond)
	t.Logf("NumGoroutine=%d GOMAXPROCS=%d", runtime.NumGoroutine(), runtime.GOMAXPROCS(0))
	_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
}
