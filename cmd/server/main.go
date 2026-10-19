package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webcrawler/internal/api"
	"webcrawler/internal/config"
	"webcrawler/internal/crawler"
	"webcrawler/internal/queue"
	"webcrawler/internal/storage"
	"webcrawler/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := storage.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()

	// Задания, оставшиеся running после аварийного завершения, вернутся в очередь.
	if n, err := st.ResetRunning(ctx); err != nil {
		return err
	} else if n > 0 {
		log.Info("requeued orphaned jobs", "count", n)
	}

	q := queue.New(cfg.QueueSize)
	cr := crawler.New(crawler.Config{
		Timeout:      cfg.RequestTimeout,
		Delay:        cfg.CrawlDelay,
		MaxBody:      cfg.MaxBodyBytes,
		AllowPrivate: cfg.AllowPrivateHosts,
	})
	pool := worker.NewPool(cfg.Workers, cfg.JobTimeout, st, q, cr, log)

	pool.Start(ctx)
	go pool.Dispatch(ctx, cfg.PollInterval)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(st, q, pool, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr, "workers", cfg.Workers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		stop()
		pool.Wait()
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	pool.Wait() // воркеры видят отмену ctx и возвращают активные задания в pending
	log.Info("bye")
	return nil
}
