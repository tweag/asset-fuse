package prefetcher

import (
	"context"
	"sync"

	"github.com/tweag/asset-fuse/internal/logging"
)

type workQueue[T, U any] struct {
	requests chan workRequest[T, U]
	workers  int
	handler  func(context.Context, T) (U, error)
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func newWorkQueue[T, U any](handler func(context.Context, T) (U, error), workers int) *workQueue[T, U] {
	q := &workQueue[T, U]{
		requests: make(chan workRequest[T, U], workqueueBufferSize),
		workers:  workers,
		handler:  handler,
	}
	return q
}

func (q *workQueue[T, U]) Start(ctx context.Context) {
	q.wg.Add(q.workers)
	for range q.workers {
		go func() {
			defer q.wg.Done()
			for req := range q.requests {
				resp, err := q.handler(ctx, req.message)
				if err != nil && len(req.callbacks) == 0 {
					logging.Errorf("background processing: %v", err)
				}
				for _, callback := range req.callbacks {
					callback(req.message, resp, err)
				}
			}
		}()
	}
}

func (q *workQueue[T, U]) Stop() {
	q.stopOnce.Do(func() {
		close(q.requests)
	})
	q.wg.Wait()
}

func (q *workQueue[T, U]) Enqueue(message T, callbacks ...func(T, U, error)) {
	q.requests <- workRequest[T, U]{message, callbacks}
}

type workRequest[T, U any] struct {
	message   T
	callbacks []func(T, U, error)
}

// workqueueBufferSize is the size of the workqueue channel
// buffer. This is a tradeoff between memory usage and
// responsiveness.
const workqueueBufferSize = 128
