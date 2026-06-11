package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigAppliesCoreDefaults(t *testing.T) {
	path := writeTempConfig(t, minimalValidConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	// 这些断言覆盖当前启动链路里最关键的默认值，避免后续重构时把保守默认配置改没。
	if cfg.Server.Port != 8080 {
		t.Fatalf("expected default server port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Server.Mode != "debug" {
		t.Fatalf("expected default server mode debug, got %q", cfg.Server.Mode)
	}
	if cfg.Database.Port != 3306 {
		t.Fatalf("expected default database port 3306, got %d", cfg.Database.Port)
	}
	if cfg.Database.Charset != "utf8mb4" {
		t.Fatalf("expected default database charset utf8mb4, got %q", cfg.Database.Charset)
	}
	if cfg.Database.MaxOpenConns != 50 {
		t.Fatalf("expected default max_open_conns 50, got %d", cfg.Database.MaxOpenConns)
	}
	if cfg.Database.MaxIdleConns != 10 {
		t.Fatalf("expected default max_idle_conns 10, got %d", cfg.Database.MaxIdleConns)
	}
	if cfg.Redis.Port != 6379 {
		t.Fatalf("expected default redis port 6379, got %d", cfg.Redis.Port)
	}
	if cfg.Redis.PoolSize != 20 {
		t.Fatalf("expected default redis pool size 20, got %d", cfg.Redis.PoolSize)
	}
	if cfg.Kafka.ConsumerGroup != "counter-agg" {
		t.Fatalf("expected default kafka consumer group counter-agg, got %q", cfg.Kafka.ConsumerGroup)
	}
	if cfg.Kafka.Topics.CounterEvents != "counter-events" {
		t.Fatalf("expected default kafka counter topic counter-events, got %q", cfg.Kafka.Topics.CounterEvents)
	}
	if cfg.Counter.Consumer.BatchSize != 100 {
		t.Fatalf("expected default counter batch size 100, got %d", cfg.Counter.Consumer.BatchSize)
	}
	if cfg.Counter.Consumer.FlushIntervalMs != 1000 {
		t.Fatalf("expected default counter flush interval 1000ms, got %d", cfg.Counter.Consumer.FlushIntervalMs)
	}
	if cfg.Auth.Password.BcryptCost != 12 {
		t.Fatalf("expected default bcrypt cost 12, got %d", cfg.Auth.Password.BcryptCost)
	}
	if cfg.Auth.Password.MinLength != 8 {
		t.Fatalf("expected default password min length 8, got %d", cfg.Auth.Password.MinLength)
	}
	if cfg.Auth.Verification.Lock.TTLMs != 5000 {
		t.Fatalf("expected default verification lock ttl 5000ms, got %d", cfg.Auth.Verification.Lock.TTLMs)
	}
	if cfg.Auth.Refresh.Lock.RetryIntervalMs != 100 {
		t.Fatalf("expected default refresh lock retry interval 100ms, got %d", cfg.Auth.Refresh.Lock.RetryIntervalMs)
	}
}

func TestValidateRejectsInvalidServerMode(t *testing.T) {
	path := writeTempConfig(t, minimalValidConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	cfg.Server.Mode = "broken"
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected invalid server mode to fail validation")
	}
	if !strings.Contains(err.Error(), "server.mode") {
		t.Fatalf("expected server.mode validation error, got %v", err)
	}
}

func TestValidateRequiresCanalHostWhenEnabled(t *testing.T) {
	path := writeTempConfig(t, minimalValidConfigYAML)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	cfg.Canal.Enabled = true
	cfg.Canal.Host = ""

	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected canal host validation failure when canal is enabled")
	}
	if !strings.Contains(err.Error(), "canal.host") {
		t.Fatalf("expected canal.host validation error, got %v", err)
	}
}

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

const minimalValidConfigYAML = `
database:
  host: 127.0.0.1
  user: root
  name: zhiguang
redis:
  host: 127.0.0.1
kafka:
  brokers:
    - 127.0.0.1:9092
auth:
  jwt:
    issuer: zhiguang
    key_id: test-key
    private_key_path: config/keys/private.pem
    public_key_path: config/keys/public.pem
`
