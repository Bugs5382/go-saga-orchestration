package storefactory_test

/*
MIT License

Copyright (c) 2026 Shane

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
	"context"
	"os"
	"testing"

	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	"github.com/Bugs5382/go-saga-orchestration/internal/storefactory"
)

func TestOpenMemory(t *testing.T) {
	cfg := config.Config{StoreType: "memory"}
	st, close, err := storefactory.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(memory): %v", err)
	}
	if st == nil {
		t.Fatal("Open(memory): returned nil store")
	}
	if err := close(); err != nil {
		t.Errorf("close(memory): %v", err)
	}
}

func TestOpenUnknownType(t *testing.T) {
	cfg := config.Config{StoreType: "badbackend"}
	_, _, err := storefactory.Open(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for unknown STORE_TYPE, got nil")
	}
}

func TestOpenRedis(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to run the redis storefactory path")
	}
	cfg := config.Config{StoreType: "redis", RedisURL: url}
	st, close, err := storefactory.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(redis): %v", err)
	}
	if st == nil {
		t.Fatal("Open(redis): returned nil store")
	}
	if err := close(); err != nil {
		t.Errorf("close(redis): %v", err)
	}
}

func TestOpenValkey(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("set TEST_REDIS_URL to run the valkey storefactory path")
	}
	cfg := config.Config{StoreType: "valkey", RedisURL: url}
	st, close, err := storefactory.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(valkey): %v", err)
	}
	if st == nil {
		t.Fatal("Open(valkey): returned nil store")
	}
	if err := close(); err != nil {
		t.Errorf("close(valkey): %v", err)
	}
}

func TestOpenPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run the postgres storefactory path")
	}
	cfg := config.Config{StoreType: "postgres", DatabaseDSN: dsn}
	st, close, err := storefactory.Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open(postgres): %v", err)
	}
	if st == nil {
		t.Fatal("Open(postgres): returned nil store")
	}
	if err := close(); err != nil {
		t.Errorf("close(postgres): %v", err)
	}
}
