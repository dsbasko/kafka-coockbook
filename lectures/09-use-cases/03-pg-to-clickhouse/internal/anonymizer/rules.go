// Package anonymizer — декларативный применятор правил из anonymize.yaml.
//
// На входе — map[string]any (поле after из Debezium event'а), на выходе —
// очищенный map с применёнными правилами. Ошибки парсинга/маппинга считаются
// фатальными для конкретного события: anonymizer пишет лог и пропускает
// сообщение (DLQ-семантика опущена сознательно — для аналитики потеря
// одного события менее критична, чем для платежей).
package anonymizer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Rule — одно правило обработки поля.
type Rule string

const (
	RuleHash        Rule = "hash"
	RuleDrop        Rule = "drop"
	RuleTruncate    Rule = "truncate"
	RulePassthrough Rule = "passthrough"
)

// TableConfig описывает обработку одной CDC-таблицы.
type TableConfig struct {
	TargetTopic string          `yaml:"target_topic"`
	Fields      map[string]Rule `yaml:"fields"`
}

// Config — корень anonymize.yaml.
type Config struct {
	Salt   string                 `yaml:"salt"`
	Tables map[string]TableConfig `yaml:"tables"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("yaml parse %s: %w", path, err)
	}
	if cfg.Tables == nil {
		return nil, fmt.Errorf("anonymize: no tables configured")
	}
	// Опечатка вроде `email: hahs` молча превращалась бы в passthrough и
	// сливала PII в analytics. Валидируем явно.
	for table, tc := range cfg.Tables {
		for field, rule := range tc.Fields {
			switch rule {
			case RuleHash, RuleDrop, RuleTruncate, RulePassthrough:
			default:
				return nil, fmt.Errorf("anonymize: table %q field %q: unknown rule %q (allowed: hash, drop, truncate, passthrough)", table, field, rule)
			}
		}
	}
	return cfg, nil
}

// Apply возвращает очищенную копию row. Поля, помеченные drop, исключаются;
// hash подменяет значение SHA-256-хексом; truncate — "{first}{lastInitial}.";
// неизвестные поля копируются как есть. Если table не описан в Config,
// возвращает ok=false — caller решает, что делать (обычно — пропустить).
func (c *Config) Apply(table string, row map[string]any) (map[string]any, bool) {
	tbl, ok := c.Tables[table]
	if !ok {
		return nil, false
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		rule, ok := tbl.Fields[k]
		if !ok {
			rule = RulePassthrough
		}
		switch rule {
		case RuleDrop:
			continue
		case RuleHash:
			out[k] = hashWithSalt(c.Salt, v)
		case RuleTruncate:
			out[k] = truncateName(v)
		case RulePassthrough:
			out[k] = v
		default:
			out[k] = v
		}
	}
	return out, true
}

// TargetTopic возвращает имя топика-назначения для table или пустую строку,
// если table не описана.
func (c *Config) TargetTopic(table string) string {
	tbl, ok := c.Tables[table]
	if !ok {
		return ""
	}
	return tbl.TargetTopic
}

func hashWithSalt(salt string, v any) string {
	if v == nil {
		return ""
	}
	s := fmt.Sprintf("%v", v)
	h := sha256.Sum256([]byte(salt + ":" + s))
	return hex.EncodeToString(h[:])
}

// truncateName: "Иван Иванов" → "ИванИ.", одно слово → как есть. Без эвристик
// на отчества и второе имя — для учебного use case'а достаточно "первое слово
// + первая буква второго". Берём строки с любыми unicode-символами через
// разбиение по пробелу.
func truncateName(v any) string {
	if v == nil {
		return ""
	}
	s := strings.TrimSpace(fmt.Sprintf("%v", v))
	if s == "" {
		return ""
	}
	parts := strings.Fields(s)
	if len(parts) == 1 {
		return parts[0]
	}
	first := parts[0]
	last := parts[1]
	runes := []rune(last)
	if len(runes) == 0 {
		return first
	}
	return first + string(runes[0]) + "."
}
