// Package limitgroup provides synchronization, error propagation, and
// resource management for groups of goroutines working on common
// subtasks that are part of the same overall task.
package limitgroup

import (
	"context"
	"runtime"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Group works exactly like a golang.org/x/sync/errgroup.Group, but limits the
// maximum number of in-flight subtasks.
//
// A zero Group is invalid. Use WithContext to construct a new Group.
type Group struct {
	limit int64
	eg    *errgroup.Group
	ctx   context.Context
	sem   *semaphore.Weighted
}

// WithContext returns a new Group and an associated Context derived from ctx.
//
// If the given limit is less than or equal to zero, a default of two times
// the number of CPUs is used.
func WithContext(ctx context.Context, limit int64) (*Group, context.Context) {
	if limit <= 0 {
		limit = int64(runtime.NumCPU() * 2)
	}
	lg := Group{limit: limit, sem: semaphore.NewWeighted(limit)}
	lg.eg, lg.ctx = errgroup.WithContext(ctx)
	return &lg, lg.ctx
}

// Go calls the given function in a new goroutine after a semphore is acquired.
// If there is an error acquiring the semaphore, the error cancels the Group
// and is returned.
//
// The first call to return a non-nil error cancels the group; its error will be
// returned by Wait.
func (lg *Group) Go(f func() error) {
	err := lg.sem.Acquire(lg.ctx, 1)
	lg.eg.Go(func() error {
		if err != nil {
			return err
		}
		defer lg.sem.Release(1)

		return f()
	})
}

// Wait blocks until all function calls from the Go method have returned,
// then returns the first non-nil error (if any) from them.
func (lg *Group) Wait() error {
	return lg.eg.Wait()
}

// Limit returns the maximum level of concurrency for the Group.
func (lg Group) Limit() int64 {
	return lg.limit
}
