package config

import (
	"os"
	"testing"
)

func TestLoadConfig_Success(t *testing.T) {
	content := []byte(`
server:
  port: 8080
  mode: debug
database:
  host: 127.0.0.1
  port: 3306
  user: root
  password: secret
  name: testdb
  charset: utf8mb4
redis:
  host: 127.0.0.1
  port: 6379
`)
	tmpFile, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	tmpFile.Close()

	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Server.Port)
	}
	if cfg.Server.Mode != "debug" {
		t.Errorf("Mode = %q, want debug", cfg.Server.Mode)
	}
	if cfg.Database.Host != "127.0.0.1" {
		t.Errorf("Database.Host = %q, want 127.0.0.1", cfg.Database.Host)
	}
	if cfg.Database.Port != 3306 {
		t.Errorf("Database.Port = %d, want 3306", cfg.Database.Port)
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("non_existent_file.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	content := []byte(`invalid: yaml: [bad`)
	tmpFile, err := os.CreateTemp(t.TempDir(), "bad-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := tmpFile.Write(content); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	tmpFile.Close()

	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestDSN_Basic(t *testing.T) {
	cfg := &DatabaseConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Name:     "testdb",
		Charset:  "utf8mb4",
	}
	dsn := cfg.DSN()
	want := "root:secret@tcp(127.0.0.1:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	if dsn != want {
		t.Errorf("DSN = %q, want %q", dsn, want)
	}
}

func TestDSN_WithTimeouts(t *testing.T) {
	cfg := &DatabaseConfig{
		Host:          "127.0.0.1",
		Port:          3306,
		User:          "root",
		Password:      "pass",
		Name:          "db",
		Charset:       "utf8mb4",
		DialTimeoutMs: 5000,
		ReadTimeoutMs: 10000,
	}
	dsn := cfg.DSN()
	if dsn != "root:pass@tcp(127.0.0.1:3306)/db?charset=utf8mb4&parseTime=True&loc=Local&timeout=5000ms&readTimeout=10000ms" {
		t.Errorf("unexpected DSN with timeouts: %q", dsn)
	}
}

func TestDSN_EmptyHost(t *testing.T) {
	cfg := &DatabaseConfig{
		Port: 3306,
		Name: "test",
	}
	dsn := cfg.DSN()
	if dsn == "" {
		t.Fatal("DSN should not be empty")
	}
}

func TestRedisAddr(t *testing.T) {
	cfg := &RedisConfig{Host: "myredis", Port: 6379}
	addr := cfg.Addr()
	if addr != "myredis:6379" {
		t.Errorf("Addr = %q, want myredis:6379", addr)
	}
}

func TestRedisAddr_EmptyHost(t *testing.T) {
	cfg := &RedisConfig{Port: 6379}
	addr := cfg.Addr()
	if addr != ":6379" {
		t.Errorf("Addr = %q, want :6379", addr)
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{123, "123"},
		{-42, "-42"},
		{999999, "999999"},
	}
	for _, tt := range tests {
		got := itoa(tt.n)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}