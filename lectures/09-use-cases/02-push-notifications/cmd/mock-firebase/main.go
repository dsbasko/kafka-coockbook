// mock-firebase — учебный HTTP-приёмник для канала Firebase use case'а 09-02.
// Имитирует сломанный downstream через FAIL_RATE_503 / FAIL_RATE_TIMEOUT.
// Stdlib-only, без зависимостей курса — Dockerfile собирает его в go-модуле,
// сгенерированном на лету.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

const channelName = "firebase"

func main() {
	addr := envOr("LISTEN_ADDR", ":8091")
	fail503 := envFloat("FAIL_RATE_503", 0.0)
	failTimeout := envFloat("FAIL_RATE_TIMEOUT", 0.0)
	latencyMS := envInt("LATENCY_MS", 5)
	timeoutHangSec := envInt("TIMEOUT_HANG_S", 30)

	logger := log.New(os.Stderr, "mock-"+channelName+" ", log.LstdFlags|log.Lmicroseconds)

	if fail503+failTimeout > 1.0 {
		logger.Fatalf("FAIL_RATE_503 + FAIL_RATE_TIMEOUT = %.2f > 1.0", fail503+failTimeout)
	}

	stats := &counters{}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := stats.snapshot()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":"%s","total":%d,"ok":%d,"fail_503":%d,"fail_timeout":%d}`+"\n",
			channelName, s.total, s.ok, s.fail503, s.failTimeout)
	})
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		stats.total.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		idem := r.Header.Get("Idempotency-Key")

		if latencyMS > 0 {
			time.Sleep(time.Duration(latencyMS) * time.Millisecond)
		}

		dice := rand.Float64()
		switch {
		case dice < fail503:
			stats.fail503.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, fmt.Sprintf(`{"name":"%s","error":"upstream temporarily unavailable"}`, channelName), http.StatusServiceUnavailable)
		case dice < fail503+failTimeout:
			stats.failTimeout.Add(1)
			select {
			case <-time.After(time.Duration(timeoutHangSec) * time.Second):
				w.WriteHeader(http.StatusGatewayTimeout)
			case <-r.Context().Done():
				return
			}
		default:
			stats.ok.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"name":"%s","status":"accepted","idempotency_key":%q}`+"\n", channelName, idem)
		}
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Printf("listening on %s (fail_503=%.2f fail_timeout=%.2f latency=%dms)",
			addr, fail503, failTimeout, latencyMS)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	s := stats.snapshot()
	logger.Printf("final: total=%d ok=%d fail_503=%d fail_timeout=%d",
		s.total, s.ok, s.fail503, s.failTimeout)
}

type counters struct {
	total       atomic.Int64
	ok          atomic.Int64
	fail503     atomic.Int64
	failTimeout atomic.Int64
}

type snapshot struct {
	total, ok, fail503, failTimeout int64
}

func (c *counters) snapshot() snapshot {
	return snapshot{
		total:       c.total.Load(),
		ok:          c.ok.Load(),
		fail503:     c.fail503.Load(),
		failTimeout: c.failTimeout.Load(),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
