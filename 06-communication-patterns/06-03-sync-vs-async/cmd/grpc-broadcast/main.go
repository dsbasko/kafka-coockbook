// grpc-broadcast — sender в синхронной fan-out схеме.
//
// Читает список URL'ов получателей из флага -targets (через запятую) или
// из env LISTENER_URLS. Для каждого «зарегистрировавшегося пользователя»
// делает unary-call Notify по очереди в каждый URL.
//
// Что важно увидеть в этой реализации (это и обсуждается в README):
//
//   - sender ЗНАЕТ все URL'ы. Чтобы добавить нового получателя — нужно
//     передеплоить sender с новым флагом. Это и есть «tight coupling»;
//   - latency сценария = сумма (или max при параллели) latency всех
//     получателей. Один медленный получатель тормозит весь broadcast;
//   - если получатель упал — событие потеряно, retry надо городить
//     руками; никакой durable-очереди тут нет;
//   - порядок дёргания получателей фиксирован, никакой replay тоже нет.
//
// Запуск: см. Makefile (`make run-grpc-broadcast`).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	usersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-03-sync-vs-async/gen/users/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	targets := flag.String("targets", "",
		"список URL'ов получателей через запятую; если пусто, берётся LISTENER_URLS")
	users := flag.Int("users", 5, "сколько UserSignedUp событий разослать")
	parallel := flag.Bool("parallel", false,
		"если true — дёргать все получатели параллельно (горутины); иначе по очереди")
	timeout := flag.Duration("timeout", 3*time.Second, "per-call timeout")
	flag.Parse()

	if *targets == "" {
		*targets = config.EnvOr("LISTENER_URLS", "")
	}
	urls := splitCSV(*targets)
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "no targets; use -targets host:port,host:port или env LISTENER_URLS")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	clients, closeAll, err := dialAll(urls)
	if err != nil {
		logger.Error("dial", "err", err)
		os.Exit(1)
	}
	defer closeAll()

	fmt.Printf("отправляем %d событий на %d получателей (parallel=%v)\n",
		*users, len(urls), *parallel)
	fmt.Println()

	for i := 0; i < *users; i++ {
		if ctx.Err() != nil {
			return
		}
		ev := mockUser(i)
		fmt.Printf("--- event #%d user_id=%s ---\n", i+1, ev.GetUserId())

		broadcast(ctx, clients, ev, *parallel, *timeout)
		fmt.Println()
	}

	fmt.Println("готово.")
}

type targetClient struct {
	url    string
	client usersv1.UserEventServiceClient
}

func dialAll(urls []string) ([]targetClient, func(), error) {
	out := make([]targetClient, 0, len(urls))
	conns := make([]*grpc.ClientConn, 0, len(urls))
	closeAll := func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}
	for _, u := range urls {
		// grpc.NewClient — lazy: реальный TCP-connect ляжет только на
		// первом RPC. Для лекции этого достаточно: если получатель не
		// запущен, мы это увидим в Notify, а не на этапе dial.
		conn, err := grpc.NewClient(u, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("dial %s: %w", u, err)
		}
		conns = append(conns, conn)
		out = append(out, targetClient{url: u, client: usersv1.NewUserEventServiceClient(conn)})
	}
	return out, closeAll, nil
}

// broadcast — собственно fan-out. В sequential-режиме видно «эффект цепи»:
// общая latency = сумма всех Notify, один медленный получатель удлиняет
// всё. В parallel-режиме общая latency ≈ max, но падают изолированно
// (через WaitGroup) и каждый промах надо обрабатывать отдельно.
func broadcast(
	ctx context.Context,
	clients []targetClient,
	ev *usersv1.UserSignedUp,
	parallel bool,
	timeout time.Duration,
) {
	if !parallel {
		for _, c := range clients {
			callOne(ctx, c, ev, timeout)
		}
		return
	}

	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func(c targetClient) {
			defer wg.Done()
			callOne(ctx, c, ev, timeout)
		}(c)
	}
	wg.Wait()
}

func callOne(ctx context.Context, c targetClient, ev *usersv1.UserSignedUp, timeout time.Duration) {
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	resp, err := c.client.Notify(rpcCtx, &usersv1.NotifyRequest{Event: ev})
	dur := time.Since(start)

	if err != nil {
		fmt.Printf("  -> %-20s FAIL  code=%s dur=%s err=%v\n",
			c.url, status.Code(err), dur, err)
		return
	}
	fmt.Printf("  -> %-20s OK    accepted=%v dur=%s\n", c.url, resp.GetAccepted(), dur)
}

func mockUser(i int) *usersv1.UserSignedUp {
	countries := []string{"KZ", "UZ", "GE", "AM", "RU"}
	return &usersv1.UserSignedUp{
		UserId:     uuid.NewString(),
		Email:      fmt.Sprintf("user-%d@example.com", i),
		Country:    countries[i%len(countries)],
		SignedUpAt: timestamppb.Now(),
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
