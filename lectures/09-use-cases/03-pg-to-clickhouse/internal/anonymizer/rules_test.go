package anonymizer

import (
	"os"
	"strings"
	"testing"
)

func TestApply_Hash(t *testing.T) {
	cfg := &Config{
		Salt: "s",
		Tables: map[string]TableConfig{
			"users": {
				TargetTopic: "analytics.users",
				Fields:      map[string]Rule{"email": RuleHash},
			},
		},
	}
	in := map[string]any{"id": int64(1), "email": "a@b.com"}
	out, ok := cfg.Apply("users", in)
	if !ok {
		t.Fatalf("unknown table")
	}
	if out["id"] != int64(1) {
		t.Errorf("passthrough id failed: %v", out["id"])
	}
	hash, _ := out["email"].(string)
	if len(hash) != 64 {
		t.Errorf("expected sha256 hex (64 chars), got %q", hash)
	}
	// Стабильность: один и тот же email с одной солью даёт один и тот же hash.
	out2, _ := cfg.Apply("users", in)
	if out2["email"] != hash {
		t.Errorf("hash not stable: %v vs %v", hash, out2["email"])
	}
	// Соль меняет hash.
	cfg2 := &Config{Salt: "x", Tables: cfg.Tables}
	out3, _ := cfg2.Apply("users", in)
	if out3["email"] == hash {
		t.Errorf("salt did not affect hash")
	}
}

func TestApply_Drop(t *testing.T) {
	cfg := &Config{
		Tables: map[string]TableConfig{
			"events": {Fields: map[string]Rule{"ip_address": RuleDrop}},
		},
	}
	out, _ := cfg.Apply("events", map[string]any{"id": 1, "ip_address": "1.2.3.4", "event_type": "click"})
	if _, ok := out["ip_address"]; ok {
		t.Errorf("drop did not remove field: %v", out)
	}
	if out["event_type"] != "click" {
		t.Errorf("passthrough lost: %v", out)
	}
}

func TestApply_Truncate(t *testing.T) {
	cfg := &Config{
		Tables: map[string]TableConfig{
			"users": {Fields: map[string]Rule{"full_name": RuleTruncate}},
		},
	}
	cases := []struct {
		in, want string
	}{
		{"Иван Иванов", "ИванИ."},
		{"  Anna  Smith  ", "AnnaS."},
		{"Single", "Single"},
		{"", ""},
	}
	for _, c := range cases {
		out, _ := cfg.Apply("users", map[string]any{"full_name": c.in})
		got, _ := out["full_name"].(string)
		if got != c.want {
			t.Errorf("truncate(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}

func TestApply_UnknownTable(t *testing.T) {
	cfg := &Config{Tables: map[string]TableConfig{"users": {}}}
	if _, ok := cfg.Apply("unknown", map[string]any{}); ok {
		t.Errorf("expected ok=false for unknown table")
	}
}

func TestTargetTopic(t *testing.T) {
	cfg := &Config{
		Tables: map[string]TableConfig{
			"users": {TargetTopic: "analytics.users"},
		},
	}
	if got := cfg.TargetTopic("users"); got != "analytics.users" {
		t.Errorf("TargetTopic users: %q", got)
	}
	if got := cfg.TargetTopic("missing"); got != "" {
		t.Errorf("TargetTopic missing: %q", got)
	}
}

func TestLoadConfig_File(t *testing.T) {
	// Минимальный smoke test: парсер читает осмысленный YAML без ошибок.
	tmp := t.TempDir() + "/anonymize.yaml"
	body := strings.TrimSpace(`
salt: "s"
tables:
  users:
    target_topic: "analytics.users"
    fields:
      email: hash
`)
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	cfg, err := LoadConfig(tmp)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Salt != "s" {
		t.Errorf("salt: %q", cfg.Salt)
	}
	if cfg.Tables["users"].Fields["email"] != RuleHash {
		t.Errorf("rule: %v", cfg.Tables["users"].Fields["email"])
	}
}

