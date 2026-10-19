// Package storage — слой работы с PostgreSQL.
package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"webcrawler/internal/model"
)

var ErrNotFound = errors.New("not found")

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// IsUUID позволяет API отсекать мусорные id до обращения к БД.
func IsUUID(s string) bool { return uuidRe.MatchString(s) }

type Store struct {
	pool *pgxpool.Pool
}

// New подключается к БД (с повторными попытками — удобно при старте
// вместе с контейнером Postgres) и применяет миграции.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		err = pool.Ping(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("ping db: %w", err)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		}
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS jobs (
		id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		url           TEXT        NOT NULL,
		max_depth     INT         NOT NULL,
		max_pages     INT         NOT NULL,
		same_domain   BOOLEAN     NOT NULL DEFAULT TRUE,
		status        TEXT        NOT NULL DEFAULT 'pending'
		              CHECK (status IN ('pending','running','done','failed','canceled')),
		error         TEXT        NOT NULL DEFAULT '',
		pages_crawled INT         NOT NULL DEFAULT 0,
		pages_failed  INT         NOT NULL DEFAULT 0,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
		started_at    TIMESTAMPTZ,
		finished_at   TIMESTAMPTZ
	)`,
	`CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at)`,
	`CREATE TABLE IF NOT EXISTS pages (
		id             BIGSERIAL PRIMARY KEY,
		job_id         UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
		url            TEXT        NOT NULL,
		depth          INT         NOT NULL,
		status_code    INT         NOT NULL DEFAULT 0,
		title          TEXT        NOT NULL DEFAULT '',
		content_type   TEXT        NOT NULL DEFAULT '',
		content_length BIGINT      NOT NULL DEFAULT 0,
		links_count    INT         NOT NULL DEFAULT 0,
		error          TEXT        NOT NULL DEFAULT '',
		duration_ms    BIGINT      NOT NULL DEFAULT 0,
		fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
		UNIQUE (job_id, url)
	)`,
}

func (s *Store) migrate(ctx context.Context) error {
	for _, m := range migrations {
		if _, err := s.pool.Exec(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// ---------- jobs ----------

const jobCols = `id::text, url, max_depth, max_pages, same_domain, status, error,
	pages_crawled, pages_failed, created_at, started_at, finished_at`

type scanner interface{ Scan(dest ...any) error }

func scanJob(row scanner) (*model.Job, error) {
	var j model.Job
	var status string
	err := row.Scan(&j.ID, &j.URL, &j.MaxDepth, &j.MaxPages, &j.SameDomain, &status, &j.Error,
		&j.PagesCrawled, &j.PagesFailed, &j.CreatedAt, &j.StartedAt, &j.FinishedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	j.Status = model.JobStatus(status)
	return &j, nil
}

func (s *Store) CreateJob(ctx context.Context, j *model.Job) error {
	var status string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO jobs (url, max_depth, max_pages, same_domain)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id::text, status, created_at`,
		j.URL, j.MaxDepth, j.MaxPages, j.SameDomain,
	).Scan(&j.ID, &status, &j.CreatedAt)
	j.Status = model.JobStatus(status)
	return err
}

func (s *Store) GetJob(ctx context.Context, id string) (*model.Job, error) {
	if !IsUUID(id) {
		return nil, ErrNotFound
	}
	return scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobCols+` FROM jobs WHERE id = $1::text::uuid`, id))
}

func (s *Store) ListJobs(ctx context.Context, status string, limit, offset int) ([]model.Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs`
	var args []any
	if status != "" {
		q += ` WHERE status = $1`
		args = append(args, status)
	}
	q += fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
	args = append(args, limit, offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []model.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

// ClaimJob атомарно переводит задание pending -> running.
// Если задание уже взято другим воркером или отменено — вернёт ErrNotFound.
func (s *Store) ClaimJob(ctx context.Context, id string) (*model.Job, error) {
	return scanJob(s.pool.QueryRow(ctx,
		`UPDATE jobs
		    SET status = 'running', started_at = now(), finished_at = NULL,
		        error = '', pages_crawled = 0, pages_failed = 0
		  WHERE id = $1::text::uuid AND status = 'pending'
		  RETURNING `+jobCols, id))
}

// FinishJob завершает running-задание с итоговым статусом.
func (s *Store) FinishJob(ctx context.Context, id string, status model.JobStatus, errMsg string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $2, error = $3, finished_at = now()
		  WHERE id = $1::text::uuid AND status = 'running'`, id, string(status), errMsg)
	return err
}

// RequeueJob возвращает running-задание в pending (graceful shutdown).
func (s *Store) RequeueJob(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'pending', started_at = NULL
		  WHERE id = $1::text::uuid AND status = 'running'`, id)
	return err
}

// CancelPending отменяет ещё не начатое задание. false — если оно уже не pending.
func (s *Store) CancelPending(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'canceled', finished_at = now()
		  WHERE id = $1::text::uuid AND status = 'pending'`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ResetRunning при старте сервера возвращает «осиротевшие» running-задания
// (процесс был убит) в очередь.
func (s *Store) ResetRunning(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'pending', started_at = NULL WHERE status = 'running'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) PendingIDs(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id::text FROM jobs WHERE status = 'pending' ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ---------- pages ----------

// AddPage сохраняет страницу и в том же запросе обновляет счётчики задания.
func (s *Store) AddPage(ctx context.Context, p model.Page) error {
	failed := 0
	if p.Error != "" {
		failed = 1
	}
	_, err := s.pool.Exec(ctx,
		`WITH ins AS (
		   INSERT INTO pages (job_id, url, depth, status_code, title, content_type,
		                      content_length, links_count, error, duration_ms, fetched_at)
		   VALUES ($1::text::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		   ON CONFLICT (job_id, url) DO NOTHING
		   RETURNING 1
		 )
		 UPDATE jobs
		    SET pages_crawled = pages_crawled + 1,
		        pages_failed  = pages_failed + $12
		  WHERE id = $1::text::uuid AND EXISTS (SELECT 1 FROM ins)`,
		p.JobID, p.URL, p.Depth, p.StatusCode, p.Title, p.ContentType,
		p.ContentLength, p.LinksCount, p.Error, p.DurationMS, p.FetchedAt, failed)
	return err
}

func (s *Store) DeletePages(ctx context.Context, jobID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM pages WHERE job_id = $1::text::uuid`, jobID)
	return err
}

func (s *Store) ListPages(ctx context.Context, jobID string, limit, offset int) ([]model.Page, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM pages WHERE job_id = $1::text::uuid`, jobID).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, job_id::text, url, depth, status_code, title, content_type,
		        content_length, links_count, error, duration_ms, fetched_at
		   FROM pages WHERE job_id = $1::text::uuid
		  ORDER BY id LIMIT $2 OFFSET $3`, jobID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	pages := []model.Page{}
	for rows.Next() {
		var p model.Page
		if err := rows.Scan(&p.ID, &p.JobID, &p.URL, &p.Depth, &p.StatusCode, &p.Title,
			&p.ContentType, &p.ContentLength, &p.LinksCount, &p.Error, &p.DurationMS, &p.FetchedAt); err != nil {
			return nil, 0, err
		}
		pages = append(pages, p)
	}
	return pages, total, rows.Err()
}
