// Package config loads go-saga-orchestration process-level configuration from
// environment variables. Both binaries (cmd/api + cmd/engine) load the
// same Config; each only reads the fields relevant to it.
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
	"time"
)

// Config is the full go-saga-orchestration configuration.
type Config struct {
	API         APIConfig
	Engine      EngineConfig
	Server      ServerConfig
	DatabaseDSN string
	RabbitMQURL string
}

// APIConfig holds knobs for the REST API server (cmd/api).
type APIConfig struct {
	Port string
}

// EngineConfig holds knobs for the engine binary (cmd/engine).
type EngineConfig struct {
	GRPCPort string
}

// ServerConfig holds shared HTTP / lifecycle knobs.
type ServerConfig struct {
	ShutdownTimeout time.Duration
}

// Load reads env vars and returns Config with defaults.
func Load() Config {
	return Config{
		API: APIConfig{
			Port: getEnv("WORKFLOW_API_PORT", "8080"),
		},
		Engine: EngineConfig{
			GRPCPort: getEnv("WORKFLOW_ENGINE_GRPC_PORT", "9090"),
		},
		Server: ServerConfig{
			ShutdownTimeout: 15 * time.Second,
		},
		DatabaseDSN: getEnv("DATABASE_DSN", ""),
		RabbitMQURL: getEnv("RABBITMQ_URL", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
