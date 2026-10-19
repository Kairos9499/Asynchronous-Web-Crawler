// Package worker содержит пул воркеров (goroutines), которые берут задания
// из очереди и выполняют обход сайтов.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"webcrawler/internal/crawler"
	"webcrawler/internal/model"
	"webcrawler/internal/queue"
	"webcrawler/internal/storage"
)

var errCanceledByUser = errors.New("canceled by user")

type Pool struct {
	store      *storage.Store
	queue      *queue.Queue
	crawler    *crawler.Crawler
	size       int
	jobTimeout time.Duration
	log        *slog.Logger

	wg      sync.WaitGroup
	mu      sync.Mutex
	cancels map[string]context.CancelCauseFunc
}

func NewPool(size int, jobTimeout time.Duration, st *storage.Store, q *queue.Queue, c *crawler.Crawler, log *slog.Logger) *Pool {
	return &Pool{
		store: st, queue: q, crawler: c,
		size: size, jobTimeout: jobTimeout, log: log,
		cancels: make(map[string]context.CancelCauseFunc),
	}
}

// Start запускает size воркеров. Они работают, пока жив ctx.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go func(n int) {
			defer p.wg.Done()
			p.run(ctx, n)
		}(i + 1)
	}
}

// Wait дожидается завершения всех воркеров (после отмены ctx).
func (p *Pool) Wait() { p.wg.Wait() }

// Cancel прерывает выполняющееся задание. false — если оно сейчас не обрабатывается.
func (p *Pool) Cancel(id string) bool {
	p.mu.Lock()
	cancel, ok := p.cancels[id]
	p.mu.Unlock()
	if ok {
		cancel(errCanceledByUser)
	}
	return ok
}

// Dispatch периодически подбирает из БД pending-задания и кладёт их в очередь.
// Это даёт устойчивость: задания переживают рестарт и переполнение очереди.
func (p *Pool) Dispatch(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		ids, err := p.store.PendingIDs(ctx, p.queue.Cap())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("dispatch: list pending", "err", err)
		}
		for _, id := range ids {
			if err := p.queue.TryEnqueue(id); err != nil {
				break // очередь полна — продолжим на следующем тике
			}
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
	}
}

func (p *Pool) run(ctx context.Context, n int) {
	log := p.log.With("worker", n)
	log.Info("worker started")
	defer log.Info("worker stopped")
	for {
		id, ok := p.queue.Next(ctx)
		if !ok {
			return
		}
		p.process(ctx, id, log)
	}
}

func (p *Pool) register(id string, cancel context.CancelCauseFunc) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.cancels[id]; exists {
		return false
	}
	p.cancels[id] = cancel
	return true
}

func (p *Pool) unregister(id string) {
	p.mu.Lock()
	delete(p.cancels, id)
	p.mu.Unlock()
}

func (p *Pool) process(ctx context.Context, id string, log *slog.Logger) {
	// Регистрируем cancel ДО claim, чтобы отмена через API не потерялась
	// в промежутке между взятием задания и началом обхода.
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if !p.register(id, cancel) {
		return // это задание уже обрабатывается
	}
	defer p.unregister(id)

	job, err := p.store.ClaimJob(ctx, id)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) && ctx.Err() == nil {
			log.Error("claim job", "job", id, "err", err)
		}
		return // уже взято/отменено/удалено
	}
	log = log.With("job", id)
	log.Info("job started", "url", job.URL, "max_depth", job.MaxDepth, "max_pages", job.MaxPages)

	// Повторный запуск (после рестарта) начинается с чистого листа.
	if err := p.store.DeletePages(ctx, id); err != nil {
		log.Error("cleanup pages", "err", err)
	}

	runCtx, stop := context.WithTimeout(jobCtx, p.jobTimeout)
	defer stop()

	crawlErr := p.crawler.Crawl(runCtx, crawler.Options{
		URL: job.URL, MaxDepth: job.MaxDepth, MaxPages: job.MaxPages, SameDomain: job.SameDomain,
	}, func(pg model.Page) {
		pg.JobID = id
		// Отдельный контекст: страницу, которую уже скачали, сохраняем даже при отмене.
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		if err := p.store.AddPage(sctx, pg); err != nil {
			log.Error("save page", "url", pg.URL, "err", err)
		}
	})

	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer fcancel()

	cause := context.Cause(runCtx)
	switch {
	case crawlErr == nil:
		err = p.store.FinishJob(fctx, id, model.StatusDone, "")
		log.Info("job done")
	case ctx.Err() != nil:
		// Сервер останавливается — вернём задание в очередь, оно продолжится после рестарта.
		err = p.store.RequeueJob(fctx, id)
		log.Info("job requeued (shutdown)")
	case errors.Is(cause, errCanceledByUser):
		err = p.store.FinishJob(fctx, id, model.StatusCanceled, "")
		log.Info("job canceled")
	case errors.Is(cause, context.DeadlineExceeded):
		err = p.store.FinishJob(fctx, id, model.StatusFailed, "job timeout exceeded")
		log.Warn("job timed out")
	default:
		err = p.store.FinishJob(fctx, id, model.StatusFailed, crawlErr.Error())
		log.Warn("job failed", "err", crawlErr)
	}
	if err != nil {
		log.Error("finalize job", "err", err)
	}
}
