// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// applyTerminalTTL queues EXPIRE commands for the run's key group inside the
// given pipeline. It is a no-op when runTTL is zero (TTL disabled).
//
// Ordering guarantee: these EXPIRE commands are queued before the run-blob
// SET ... KEEPTTL that follows in the same MULTI/EXEC pipeline. EXPIRE
// therefore sets the TTL on the already-existing key, and the subsequent SET
// with KEEPTTL preserves that TTL rather than clearing it. Reversing the order
// would cause KEEPTTL to inherit a -1 (no expiry) instead of the intended TTL.
func (s *Store) applyTerminalTTL(ctx context.Context, p goredis.Pipeliner, runID uuid.UUID) {
	if s.runTTL <= 0 {
		return
	}
	id := runID.String()
	p.Expire(ctx, s.key("run", id), s.runTTL)
	p.Expire(ctx, s.key("events", id), s.runTTL)
	p.Expire(ctx, s.key("signals", id), s.runTTL)
	p.Expire(ctx, s.key("idx", "usertasks", "byrun", id), s.runTTL)
}
