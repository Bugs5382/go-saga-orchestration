// Package storefactory selects and opens a store.Store backend based on
// the STORE_TYPE environment variable. Supported backends: postgres (default),
// redis, valkey, memory.
package storefactory

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
	"fmt"

	"github.com/Bugs5382/go-saga-orchestration/internal/config"
	"github.com/Bugs5382/go-saga-orchestration/store"
	"github.com/Bugs5382/go-saga-orchestration/store/memory"
	"github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"github.com/Bugs5382/go-saga-orchestration/store/redis"
)

// Open selects, opens, and optionally migrates a store backend based on
// cfg.StoreType. It returns the store, a close function (always non-nil),
// and any error. The caller must invoke close when the process shuts down.
func Open(ctx context.Context, cfg config.Config) (store.Store, func() error, error) {
	switch cfg.StoreType {
	case "", "postgres":
		st, err := postgres.Open(ctx, cfg.DatabaseDSN)
		if err != nil {
			return nil, nil, err
		}
		if err := postgres.Migrate(cfg.DatabaseDSN); err != nil {
			st.Close()
			return nil, nil, err
		}
		return st, func() error { st.Close(); return nil }, nil

	case "redis", "valkey":
		st, err := redis.Open(ctx, cfg.RedisURL, redis.WithRunTTL(cfg.RedisRunTTL))
		if err != nil {
			return nil, nil, err
		}
		return st, st.Close, nil

	case "memory":
		return memory.New(), func() error { return nil }, nil

	default:
		return nil, nil, fmt.Errorf("unknown STORE_TYPE %q (want postgres|redis|valkey|memory)", cfg.StoreType)
	}
}
