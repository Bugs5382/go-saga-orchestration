package config

/*
MIT License

Copyright (c) 2026 Bugs5382

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	_ = os.Unsetenv("WORKFLOW_API_PORT")
	_ = os.Unsetenv("WORKFLOW_ENGINE_GRPC_PORT")
	_ = os.Unsetenv("DATABASE_DSN")
	_ = os.Unsetenv("RABBITMQ_URL")

	cfg := Load()
	if cfg.API.Port != "8080" {
		t.Errorf("API.Port = %q, want %q", cfg.API.Port, "8080")
	}
	if cfg.Engine.GRPCPort != "9090" {
		t.Errorf("Engine.GRPCPort = %q, want %q", cfg.Engine.GRPCPort, "9090")
	}
	if cfg.Server.ShutdownTimeout != 15*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 15s", cfg.Server.ShutdownTimeout)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("WORKFLOW_API_PORT", "9000")
	t.Setenv("DATABASE_DSN", "postgres://localhost/x")

	cfg := Load()
	if cfg.API.Port != "9000" {
		t.Errorf("API.Port = %q, want %q", cfg.API.Port, "9000")
	}
	if cfg.DatabaseDSN != "postgres://localhost/x" {
		t.Errorf("DatabaseDSN = %q, want overridden value", cfg.DatabaseDSN)
	}
}

func TestLoadStoreType(t *testing.T) {
	t.Setenv("STORE_TYPE", "redis")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("REDIS_RUN_TTL", "1h")

	cfg := Load()
	if cfg.StoreType != "redis" {
		t.Errorf("StoreType = %q, want %q", cfg.StoreType, "redis")
	}
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Errorf("RedisURL = %q, want overridden value", cfg.RedisURL)
	}
	if cfg.RedisRunTTL != time.Hour {
		t.Errorf("RedisRunTTL = %v, want 1h", cfg.RedisRunTTL)
	}
}

func TestLoadStoreTypeDefaults(t *testing.T) {
	for _, key := range []string{"STORE_TYPE", "REDIS_URL", "REDIS_RUN_TTL"} {
		prev, had := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		key, prev, had := key, prev, had // capture
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(key, prev)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}

	cfg := Load()
	if cfg.StoreType != "postgres" {
		t.Errorf("StoreType default = %q, want %q", cfg.StoreType, "postgres")
	}
	if cfg.RedisURL != "" {
		t.Errorf("RedisURL default = %q, want empty", cfg.RedisURL)
	}
	if cfg.RedisRunTTL != 0 {
		t.Errorf("RedisRunTTL default = %v, want 0", cfg.RedisRunTTL)
	}
}

func TestParseRunTTLBadValue(t *testing.T) {
	t.Setenv("REDIS_RUN_TTL", "not-a-duration")

	cfg := Load()
	if cfg.RedisRunTTL != 0 {
		t.Errorf("RedisRunTTL bad parse = %v, want 0 (default)", cfg.RedisRunTTL)
	}
}

func TestLoadCronDispatcherDefault(t *testing.T) {
	_ = os.Unsetenv("WORKFLOW_CRON_DISPATCHER")

	cfg := Load()
	if !cfg.Engine.EnableCronDispatcher {
		t.Errorf("EnableCronDispatcher default = false, want true (current behavior)")
	}
}

func TestLoadCronDispatcherDisabled(t *testing.T) {
	t.Setenv("WORKFLOW_CRON_DISPATCHER", "false")

	cfg := Load()
	if cfg.Engine.EnableCronDispatcher {
		t.Errorf("EnableCronDispatcher = true, want false when WORKFLOW_CRON_DISPATCHER=false")
	}
}

func TestLoadCronDispatcherEnabled(t *testing.T) {
	t.Setenv("WORKFLOW_CRON_DISPATCHER", "true")

	cfg := Load()
	if !cfg.Engine.EnableCronDispatcher {
		t.Errorf("EnableCronDispatcher = false, want true when WORKFLOW_CRON_DISPATCHER=true")
	}
}

func TestLoadCronDispatcherBadValue(t *testing.T) {
	t.Setenv("WORKFLOW_CRON_DISPATCHER", "not-a-bool")

	cfg := Load()
	if !cfg.Engine.EnableCronDispatcher {
		t.Errorf("EnableCronDispatcher bad parse = false, want true (default)")
	}
}
