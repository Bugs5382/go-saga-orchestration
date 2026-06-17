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
