// Package redis is a Redis/Valkey-backed store.Store implementation.
package redis

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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Bugs5382/go-saga-orchestration/domain"
	"github.com/Bugs5382/go-saga-orchestration/store"
)

const defaultPrefix = "saga:"
const txMaxRetries = 50

// errAbortNoWrite is returned by a mutate func to abort a txRun without
// writing anything (idempotent no-op). txRun treats it as success.
var errAbortNoWrite = errors.New("redis: abort tx without write")

// Store is a Redis/Valkey-backed store.Store. Safe for concurrent use.
type Store struct {
	rdb    goredis.UniversalClient
	prefix string
	runTTL time.Duration // 0 = no expiry on terminal runs
}

// Option configures a Store.
type Option func(*Store)

// WithPrefix overrides the default "saga:" key prefix.
func WithPrefix(p string) Option { return func(s *Store) { s.prefix = p } }

// WithRunTTL sets the terminal-run expiry (0 disables).
func WithRunTTL(d time.Duration) Option { return func(s *Store) { s.runTTL = d } }

// Open dials url (redis:// or rediss://; Redis or Valkey) and verifies it.
func Open(ctx context.Context, url string, opts ...Option) (*Store, error) {
	opt, err := goredis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	s := &Store{rdb: goredis.NewClient(opt), prefix: defaultPrefix}
	for _, o := range opts {
		o(s)
	}
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close releases the client.
func (s *Store) Close() error { return s.rdb.Close() }

var _ store.Store = (*Store)(nil)

func (s *Store) key(parts ...string) string {
	k := s.prefix
	for i, p := range parts {
		if i > 0 {
			k += ":"
		}
		k += p
	}
	return k
}

// getJSON loads key and unmarshals into T. ok=false when the key is absent.
func getJSON[T any](ctx context.Context, rdb goredis.Cmdable, key string) (T, bool, error) {
	var v T
	b, err := rdb.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	return v, true, unmarshalJSON(b, &v)
}

// unmarshalJSON decodes JSON bytes into dest. Numbers inside map[string]any
// and []any fields are decoded as int (whole numbers) or float64 rather than
// float64 for all numbers as standard json.Unmarshal does.
func unmarshalJSON(b []byte, dest any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(dest); err != nil {
		return err
	}
	// Walk the decoded value and convert json.Number leaves inside any
	// map[string]any or []any fields to native int or float64.
	walkAndConvert(reflect.ValueOf(dest))
	return nil
}

// walkAndConvert recursively walks rv and converts json.Number values inside
// map[string]any and []any to int or float64.
func walkAndConvert(rv reflect.Value) {
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface:
		if !rv.IsNil() {
			walkAndConvert(rv.Elem())
		}
	case reflect.Struct:
		for i := 0; i < rv.NumField(); i++ {
			walkAndConvert(rv.Field(i))
		}
	case reflect.Slice:
		if rv.Type() == reflect.TypeOf([]any(nil)) {
			for i := 0; i < rv.Len(); i++ {
				elem := rv.Index(i)
				if converted, ok := convertJSONNumber(elem.Elem()); ok {
					elem.Set(reflect.ValueOf(converted))
				} else {
					walkAndConvert(elem)
				}
			}
		} else {
			for i := 0; i < rv.Len(); i++ {
				walkAndConvert(rv.Index(i))
			}
		}
	case reflect.Map:
		if rv.Type() == reflect.TypeOf(map[string]any(nil)) {
			for _, k := range rv.MapKeys() {
				v := rv.MapIndex(k)
				if converted, ok := convertJSONNumber(v.Elem()); ok {
					rv.SetMapIndex(k, reflect.ValueOf(converted))
				} else {
					// Recurse into nested maps/slices via a temporary.
					tmp := v.Elem()
					walkAndConvert(tmp)
				}
			}
		}
	}
}

// convertJSONNumber converts a json.Number reflect.Value to int or float64.
// Returns (converted, true) if rv holds a json.Number, (nil, false) otherwise.
func convertJSONNumber(rv reflect.Value) (any, bool) {
	if !rv.IsValid() {
		return nil, false
	}
	num, ok := rv.Interface().(json.Number)
	if !ok {
		return nil, false
	}
	if i, err := num.Int64(); err == nil {
		return int(i), true
	}
	if f, err := num.Float64(); err == nil {
		return f, true
	}
	return string(num), true
}

// txRun runs mutate against the run under WATCH/MULTI, persisting the result.
// mutate may return store.ErrNotFound or errAbortNoWrite to abort without writing.
func (s *Store) txRun(ctx context.Context, runID uuid.UUID, mutate func(r *domain.SagaRun, p goredis.Pipeliner) error) error {
	rk := s.key("run", runID.String())
	for i := 0; i < txMaxRetries; i++ {
		err := s.rdb.Watch(ctx, func(tx *goredis.Tx) error {
			r, ok, err := getJSON[domain.SagaRun](ctx, tx, rk)
			if err != nil {
				return err
			}
			if !ok {
				return store.ErrNotFound{Entity: "saga_run", ID: runID.String()}
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				if err := mutate(&r, p); err != nil {
					return err
				}
				b, mErr := json.Marshal(r)
				if mErr != nil {
					return mErr
				}
				return p.Set(ctx, rk, b, goredis.KeepTTL).Err()
			})
			return err
		}, rk)
		if errors.Is(err, errAbortNoWrite) {
			return nil
		}
		if errors.Is(err, goredis.TxFailedErr) {
			continue // optimistic conflict; retry
		}
		return err
	}
	return errors.New("redis: tx retry budget exhausted for run " + runID.String())
}

// --- store.Store method stubs (remaining unimplemented groups) ---

func (s *Store) ListRuns(_ context.Context, _ store.RunFilter) ([]domain.SagaRun, error) {
	panic("not implemented: ListRuns")
}

func (s *Store) CountRuns(_ context.Context, _ store.RunFilter) (int, error) {
	panic("not implemented: CountRuns")
}

func (s *Store) StatsForWorkflow(_ context.Context, _ string) (store.WorkflowStats, error) {
	panic("not implemented: StatsForWorkflow")
}

func (s *Store) SetPausedWithWakeup(_ context.Context, _ uuid.UUID, _ time.Time) error {
	panic("not implemented: SetPausedWithWakeup")
}

func (s *Store) SetPausedAwaitingSignal(_ context.Context, _ uuid.UUID, _ string, _ *time.Time) error {
	panic("not implemented: SetPausedAwaitingSignal")
}

func (s *Store) SetPausedAwaitingEvent(_ context.Context, _ uuid.UUID, _ string, _ map[string]string) error {
	panic("not implemented: SetPausedAwaitingEvent")
}

func (s *Store) SetPausedAwaitingEventWithDeadline(_ context.Context, _ uuid.UUID, _ string, _ map[string]string, _ *time.Time) error {
	panic("not implemented: SetPausedAwaitingEventWithDeadline")
}

func (s *Store) ClearPause(_ context.Context, _ uuid.UUID) error {
	panic("not implemented: ClearPause")
}

func (s *Store) FindRunsByDueWakeup(_ context.Context, _ time.Time, _ int) ([]uuid.UUID, error) {
	panic("not implemented: FindRunsByDueWakeup")
}

func (s *Store) FindRunsByAwaitedEvent(_ context.Context, _ string) ([]domain.SagaRun, error) {
	panic("not implemented: FindRunsByAwaitedEvent")
}

func (s *Store) TryConsumeAwaitedSignal(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	panic("not implemented: TryConsumeAwaitedSignal")
}

func (s *Store) AppendSignal(_ context.Context, _ domain.SagaSignal) error {
	panic("not implemented: AppendSignal")
}

func (s *Store) WakeFromExternal(_ context.Context, _ uuid.UUID) error {
	panic("not implemented: WakeFromExternal")
}

func (s *Store) SpawnChildRun(_ context.Context, _ uuid.UUID, _, _ string, _ domain.WorkflowDefinition, _ map[string]any) (uuid.UUID, error) {
	panic("not implemented: SpawnChildRun")
}

func (s *Store) SpawnChildRunAt(_ context.Context, _ uuid.UUID, _, _ string, _ domain.WorkflowDefinition, _ map[string]any, _ string) (uuid.UUID, error) {
	panic("not implemented: SpawnChildRunAt")
}

func (s *Store) ListChildrenByParent(_ context.Context, _ uuid.UUID, _ string) ([]domain.SagaRun, error) {
	panic("not implemented: ListChildrenByParent")
}

func (s *Store) PushTryCatch(_ context.Context, _ uuid.UUID, _ domain.TryCatchFrame) error {
	panic("not implemented: PushTryCatch")
}

func (s *Store) PopTryCatch(_ context.Context, _ uuid.UUID) (domain.TryCatchFrame, bool, error) {
	panic("not implemented: PopTryCatch")
}

func (s *Store) CreateUserTask(_ context.Context, _ domain.UserTask) error {
	panic("not implemented: CreateUserTask")
}

func (s *Store) GetUserTask(_ context.Context, _ uuid.UUID) (domain.UserTask, error) {
	panic("not implemented: GetUserTask")
}

func (s *Store) SubmitUserTask(_ context.Context, _ uuid.UUID, _ string, _ map[string]any) error {
	panic("not implemented: SubmitUserTask")
}

func (s *Store) ListUserTasksByRun(_ context.Context, _ uuid.UUID) ([]domain.UserTask, error) {
	panic("not implemented: ListUserTasksByRun")
}

func (s *Store) UpsertActionRegistration(_ context.Context, _ domain.ActionRegistration) error {
	panic("not implemented: UpsertActionRegistration")
}

func (s *Store) ListActions(_ context.Context, _ store.ActionFilter) ([]domain.ActionRegistration, error) {
	panic("not implemented: ListActions")
}

func (s *Store) GetAction(_ context.Context, _, _ string, _ int) (domain.ActionRegistration, error) {
	panic("not implemented: GetAction")
}

func (s *Store) UpsertTrigger(_ context.Context, _ domain.SagaTrigger) (uuid.UUID, error) {
	panic("not implemented: UpsertTrigger")
}

func (s *Store) GetTrigger(_ context.Context, _ uuid.UUID) (domain.SagaTrigger, error) {
	panic("not implemented: GetTrigger")
}

func (s *Store) ListTriggers(_ context.Context, _ store.TriggerFilter) ([]domain.SagaTrigger, error) {
	panic("not implemented: ListTriggers")
}

func (s *Store) DeleteTrigger(_ context.Context, _ uuid.UUID) error {
	panic("not implemented: DeleteTrigger")
}

func (s *Store) MarkAwaitingAction(_ context.Context, _ uuid.UUID, _ string, _ int) error {
	panic("not implemented: MarkAwaitingAction")
}

func (s *Store) CompleteAction(_ context.Context, _ uuid.UUID, _ int, _ map[string]any) error {
	panic("not implemented: CompleteAction")
}

func (s *Store) FailAction(_ context.Context, _ uuid.UUID, _ int, _, _ string, _ bool) error {
	panic("not implemented: FailAction")
}
