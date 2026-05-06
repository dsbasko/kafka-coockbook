// Package config — крошечная обёртка над os.Getenv для лекций.
//
// MustEnv валит процесс, если переменная не задана: в учебном коде это лучше,
// чем тихо подставить дефолт и получить непонятную ошибку дальше. EnvOr
// удобен там, где дефолт реально безопасен (KAFKA_BOOTSTRAP, LOG_LEVEL и т.п.).
package config

import (
	"fmt"
	"os"
)

// MustEnv возвращает значение переменной окружения или паникует, если
// переменная пуста или не задана.
func MustEnv(name string) string {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		panic(fmt.Sprintf("config.MustEnv: %s is required but empty", name))
	}
	return v
}

// EnvOr возвращает значение переменной окружения или fallback, если
// переменная пуста или не задана.
func EnvOr(name, fallback string) string {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return fallback
	}
	return v
}
