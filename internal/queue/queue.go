// Package queue реализует асинхронную очередь идентификаторов заданий
// на основе буферизованного канала с защитой от дублей.
package queue

import (
	"context"
	"errors"
	"sync"
)

var ErrFull = errors.New("queue is full")

type Queue struct {
	ch     chan string
	mu     sync.Mutex
	queued map[string]struct{}
}

func New(size int) *Queue {
	return &Queue{
		ch:     make(chan string, size),
		queued: make(map[string]struct{}, size),
	}
}

// TryEnqueue не блокируется: если очередь полна, возвращает ErrFull.
// Само задание при этом остаётся в БД со статусом pending и будет
// подобрано диспетчером позже. Повторная постановка того же id — no-op.
func (q *Queue) TryEnqueue(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.queued[id]; ok {
		return nil
	}
	select {
	case q.ch <- id:
		q.queued[id] = struct{}{}
		return nil
	default:
		return ErrFull
	}
}

// Next блокируется до появления задания или отмены контекста.
func (q *Queue) Next(ctx context.Context) (string, bool) {
	select {
	case id := <-q.ch:
		q.mu.Lock()
		delete(q.queued, id)
		q.mu.Unlock()
		return id, true
	case <-ctx.Done():
		return "", false
	}
}

func (q *Queue) Cap() int { return cap(q.ch) }
