// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package storage

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

const burstLimit = 1000 * 1000 * 1000

type withRatelimit struct {
	ExternalStorage
	Ratelimit uint64 // Byte/sec
}

// WithRatelimit
func WithRatelimit(inner ExternalStorage, ratelimit uint64) ExternalStorage {
	if ratelimit == 0 {
		return inner
	}

	s := &withRatelimit{ExternalStorage: inner, Ratelimit: ratelimit}
	return s
}

func (r *withRatelimit) Create(ctx context.Context, name string) (ExternalFileWriter, error) {
	inner, err := r.ExternalStorage.Create(ctx, name)
	if err != nil {
		return nil, err
	}
	w := &withRateLimitWriter{
		ExternalFileWriter: inner,
	}
	if r.Ratelimit > 0 {
		w.limiter = rate.NewLimiter(rate.Limit(r.Ratelimit), burstLimit)
		w.limiter.AllowN(time.Now(), burstLimit)
	}
	return w, nil
}

type withRateLimitWriter struct {
	ExternalFileWriter
	limiter *rate.Limiter
}

func (rw *withRateLimitWriter) Write(ctx context.Context, p []byte) (int, error) {
	// do rate limiting here.
	if rw.limiter == nil {
		return rw.ExternalFileWriter.Write(ctx, p)
	}
	n, err := rw.ExternalFileWriter.Write(ctx, p)
	if err != nil {
		return n, err
	}
	if err := rw.limiter.WaitN(ctx, n); err != nil {
		return n, err
	}
	return n, err
}
