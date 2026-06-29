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
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
)

// SetPausedWithWakeup marks the run paused with a wakeup time and registers it
// in the idx:wakeup ZSET.
func (s *Store) SetPausedWithWakeup(ctx context.Context, runID uuid.UUID, wakeupAt time.Time) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		r.State = domain.RunStatePaused
		w := wakeupAt
		r.WakeupAt = &w
		p.ZAdd(ctx, s.key("idx", "wakeup"), goredis.Z{
			Score:  float64(wakeupAt.UnixNano()),
			Member: runID.String(),
		})
		return nil
	})
}

// SetPausedAwaitingSignal marks the run paused awaiting signalName, with an
// optional deadline added to idx:wakeup.
func (s *Store) SetPausedAwaitingSignal(ctx context.Context, runID uuid.UUID, signalName string, deadline *time.Time) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		r.State = domain.RunStatePaused
		name := signalName
		r.AwaitedSignal = &name
		if deadline != nil {
			d := *deadline
			r.WakeupAt = &d
			p.ZAdd(ctx, s.key("idx", "wakeup"), goredis.Z{
				Score:  float64(d.UnixNano()),
				Member: runID.String(),
			})
		}
		return nil
	})
}

// SetPausedAwaitingEvent marks the run paused awaiting an event on topic with
// the given header filter. The run is added to idx:awaitevent:{topic}.
func (s *Store) SetPausedAwaitingEvent(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string) error {
	return s.SetPausedAwaitingEventWithDeadline(ctx, runID, topic, headers, nil)
}

// SetPausedAwaitingEventWithDeadline is SetPausedAwaitingEvent plus an optional
// wakeup deadline which is also recorded in idx:wakeup.
func (s *Store) SetPausedAwaitingEventWithDeadline(ctx context.Context, runID uuid.UUID, topic string, headers map[string]string, deadline *time.Time) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		r.State = domain.RunStatePaused
		t := topic
		r.AwaitedEventTopic = &t
		hdrs := map[string]string{}
		for k, v := range headers {
			hdrs[k] = v
		}
		r.AwaitedEventHeaders = hdrs
		r.WakeupAt = deadline
		p.SAdd(ctx, s.key("idx", "awaitevent", topic), runID.String())
		if deadline != nil {
			p.ZAdd(ctx, s.key("idx", "wakeup"), goredis.Z{
				Score:  float64(deadline.UnixNano()),
				Member: runID.String(),
			})
		}
		return nil
	})
}

// ClearPause transitions the run to running, clears all await markers and
// wakeup_at, and removes the run from idx:wakeup and idx:awaitevent:{topic}.
func (s *Store) ClearPause(ctx context.Context, runID uuid.UUID) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		// Capture the topic before clearing it (needed to remove from the index).
		if r.AwaitedEventTopic != nil {
			p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), runID.String())
		}
		p.ZRem(ctx, s.key("idx", "wakeup"), runID.String())
		r.State = domain.RunStateRunning
		r.WakeupAt = nil
		r.AwaitedSignal = nil
		r.AwaitedEventTopic = nil
		r.AwaitedEventHeaders = nil
		return nil
	})
}

// WakeFromExternal clears all await markers and wakeup_at while leaving the
// run state as paused, then removes it from idx:wakeup / idx:awaitevent.
func (s *Store) WakeFromExternal(ctx context.Context, runID uuid.UUID) error {
	return s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.AwaitedEventTopic != nil {
			p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), runID.String())
		}
		p.ZRem(ctx, s.key("idx", "wakeup"), runID.String())
		// Leave state=paused; clear all await markers + wakeup_at.
		r.WakeupAt = nil
		r.AwaitedSignal = nil
		r.AwaitedEventTopic = nil
		r.AwaitedEventHeaders = nil
		return nil
	})
}

// FindRunsByDueWakeup returns up to limit run IDs of paused runs whose
// wakeup_at is at or before now. It queries idx:wakeup via ZRANGEBYSCORE then
// loads the candidate run blobs and retains only those with State==RunStatePaused,
// matching the memory-store oracle (store/memory/store.go). The limit bounds the
// result count, not the ZSET scan range.
func (s *Store) FindRunsByDueWakeup(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	// Over-fetch from the ZSET (no server-side limit) so we can apply the
	// state filter and still return up to limit paused entries.
	members, err := s.rdb.ZRangeByScore(ctx, s.key("idx", "wakeup"), &goredis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(now.UnixNano(), 10),
		Offset: 0,
		Count:  0, // 0 means no server-side limit; we filter locally
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []uuid.UUID{}, nil
	}
	keys := make([]string, len(members))
	memberIDs := make([]uuid.UUID, len(members))
	for i, m := range members {
		id, err := uuid.Parse(m)
		if err != nil {
			continue
		}
		memberIDs[i] = id
		keys[i] = s.key("run", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, limit)
	for i, v := range vals {
		if len(ids) >= limit {
			break
		}
		if v == nil {
			continue
		}
		var r domain.SagaRun
		if err := json.Unmarshal([]byte(v.(string)), &r); err != nil {
			continue
		}
		if r.State == domain.RunStatePaused {
			ids = append(ids, memberIDs[i])
		}
	}
	return ids, nil
}

// FindRunsByAwaitedEvent returns all paused runs awaiting an event on topic by
// loading members of idx:awaitevent:{topic} and returning those whose run blob
// is still paused and awaiting that topic (defensive filter).
func (s *Store) FindRunsByAwaitedEvent(ctx context.Context, topic string) ([]domain.SagaRun, error) {
	members, err := s.rdb.SMembers(ctx, s.key("idx", "awaitevent", topic)).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return []domain.SagaRun{}, nil
	}
	keys := make([]string, len(members))
	for i, m := range members {
		keys[i] = s.key("run", m)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := []domain.SagaRun{}
	for _, v := range vals {
		if v == nil {
			continue
		}
		var r domain.SagaRun
		if err := json.Unmarshal([]byte(v.(string)), &r); err != nil {
			continue
		}
		// Defensive: only include runs still paused and awaiting this topic.
		if r.State == domain.RunStatePaused && r.AwaitedEventTopic != nil && *r.AwaitedEventTopic == topic {
			out = append(out, r)
		}
	}
	return out, nil
}

// TryConsumeAwaitedSignal attempts to consume the awaited signal on the run.
// Returns (false, nil) when the run is missing, not paused, or the awaited
// signal name does not match. On match it clears all await markers and wakeup_at,
// marks the first unconsumed matching signal as consumed, and returns (true, nil).
func (s *Store) TryConsumeAwaitedSignal(ctx context.Context, runID uuid.UUID, signalName string) (bool, error) {
	var consumed bool
	err := s.txRun(ctx, runID, func(r *domain.SagaRun, p goredis.Pipeliner) error {
		if r.State != domain.RunStatePaused || r.AwaitedSignal == nil || *r.AwaitedSignal != signalName {
			// No-op: return sentinel so txRun does not rewrite the blob.
			return errAbortNoWrite
		}
		// Remove from indexes before clearing the fields.
		if r.AwaitedEventTopic != nil {
			p.SRem(ctx, s.key("idx", "awaitevent", *r.AwaitedEventTopic), runID.String())
		}
		p.ZRem(ctx, s.key("idx", "wakeup"), runID.String())

		// Clear all await markers and wakeup_at.
		r.AwaitedSignal = nil
		r.AwaitedEventTopic = nil
		r.AwaitedEventHeaders = nil
		r.WakeupAt = nil

		// Mark the first unconsumed matching signal as consumed.
		// Note: the signals list is read via LRANGE on the base client (not
		// inside the WATCH), so it is not part of the optimistic transaction.
		// A concurrent AppendSignal for the same run between this read and the
		// subsequent DEL+RPUSH rewrite could lose that signal. This is
		// acceptable for v1 and matches the memory store's non-serialization
		// of signals vs the run blob.
		sigKey := s.key("signals", runID.String())
		rawSigs, err := s.rdb.LRange(ctx, sigKey, 0, -1).Result()
		if err != nil {
			return err
		}
		sigs := make([]domain.SagaSignal, 0, len(rawSigs))
		for _, raw := range rawSigs {
			var sig domain.SagaSignal
			if err := json.Unmarshal([]byte(raw), &sig); err != nil {
				return err
			}
			sigs = append(sigs, sig)
		}
		now := time.Now().UTC()
		for i := range sigs {
			if sigs[i].SignalName == signalName && sigs[i].ConsumedAt == nil {
				sigs[i].ConsumedAt = &now
			}
		}
		if len(rawSigs) > 0 {
			// Rewrite the signals list atomically: DEL then RPUSH in the pipeline.
			p.Del(ctx, sigKey)
			for _, sig := range sigs {
				b, err := json.Marshal(sig)
				if err != nil {
					return err
				}
				p.RPush(ctx, sigKey, b)
			}
		}
		consumed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return consumed, nil
}

// AppendSignal appends sig to the signals:{runID} list.
func (s *Store) AppendSignal(ctx context.Context, sig domain.SagaSignal) error {
	b, err := json.Marshal(sig)
	if err != nil {
		return err
	}
	return s.rdb.RPush(ctx, s.key("signals", sig.RunID.String()), b).Err()
}
