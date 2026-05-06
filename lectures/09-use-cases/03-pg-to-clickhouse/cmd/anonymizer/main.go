// anonymizer — consumer на cdc.public.* (Debezium output),
// применяет anonymize.yaml и пишет очищенные события в analytics.* топики.
//
// Для удобства тестов вся логика вынесена в internal/anonymizer; здесь —
// только CLI-обёртка с разбором флагов и запуском Run.
package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"strings"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/03-pg-to-clickhouse/internal/anonymizer"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	configPath := flag.String("config", "anonymize.yaml", "путь к anonymize.yaml")
	nodeID := flag.String("node-id", "anonymizer-1", "ID ноды для логов и client-id")
	group := flag.String("group", "usecase-09-03-anonymizer", "consumer group")
	regex := flag.String("source-regex", `^cdc\.public\.(users|orders|events)$`,
		"regex для топиков-источников (Debezium output)")
	fromStart := flag.Bool("from-start", false, "при первом запуске группы читать с начала")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cfg, err := anonymizer.LoadConfig(*configPath)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}
	logger.Info("config loaded",
		"path", *configPath,
		"tables", len(cfg.Tables),
		"salt_len", len(cfg.Salt))

	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP",
		"localhost:19092,localhost:19093,localhost:19094")
	seeds := splitCSV(bootstrap)

	if err := anonymizer.Run(ctx, anonymizer.RunOpts{
		NodeID:      *nodeID,
		Bootstrap:   seeds,
		Group:       *group,
		SourceRegex: *regex,
		Config:      cfg,
		FromStart:   *fromStart,
		Logger:      logger,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("anonymizer failed", "err", err)
		os.Exit(1)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
