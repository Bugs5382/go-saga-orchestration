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
	case reflect.Pointer, reflect.Interface:
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
