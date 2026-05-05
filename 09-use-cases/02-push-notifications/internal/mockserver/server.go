// Package mockserver — общий HTTP-приёмник для mock-firebase / mock-apns /
// mock-webhook. Имитирует сломанный downstream через FAIL_RATE_503 и
// FAIL_RATE_TIMEOUT. Тестам тоже удобен: они импортируют Handler() и
// крутят свой httptest.Server без отдельного процесса.
//
// Этот пакет используется только в тестах. cmd/mock-* binaries — самостоятельные
// stdlib-only main.go (см. cmd/mock-*/main.go и Dockerfile рядом с ними),
// чтобы Dockerfile собирал их без подмодулей курса.
package mockserver

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"
)

type Stats struct {
	Total       atomic.Int64
	OK          atomic.Int64
	Fail503     atomic.Int64
	FailTimeout atomic.Int64
}

type Config struct {
	Name        string
	Fail503     float64
	FailTimeout float64
	LatencyMS   int
	HangSec     int
}

func Handler(cfg Config, stats *Stats) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"name":%q,"total":%d,"ok":%d,"fail_503":%d,"fail_timeout":%d}`+"\n",
			cfg.Name, stats.Total.Load(), stats.OK.Load(), stats.Fail503.Load(), stats.FailTimeout.Load())
	})
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		stats.Total.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()

		if cfg.LatencyMS > 0 {
			time.Sleep(time.Duration(cfg.LatencyMS) * time.Millisecond)
		}

		dice := rand.Float64()
		switch {
		case dice < cfg.Fail503:
			stats.Fail503.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, fmt.Sprintf(`{"name":%q,"error":"upstream temporarily unavailable"}`, cfg.Name), http.StatusServiceUnavailable)
		case dice < cfg.Fail503+cfg.FailTimeout:
			stats.FailTimeout.Add(1)
			hang := cfg.HangSec
			if hang <= 0 {
				hang = 30
			}
			select {
			case <-time.After(time.Duration(hang) * time.Second):
				w.WriteHeader(http.StatusGatewayTimeout)
			case <-r.Context().Done():
				return
			}
		default:
			stats.OK.Add(1)
			idem := r.Header.Get("Idempotency-Key")
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"name":%q,"status":"accepted","idempotency_key":%q}`+"\n", cfg.Name, idem)
		}
	})
	return mux
}
