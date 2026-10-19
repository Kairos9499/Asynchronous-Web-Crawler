// Package api — REST API поверх net/http (Go 1.22+ ServeMux с методами и {wildcards}).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"webcrawler/internal/model"
	"webcrawler/internal/queue"
	"webcrawler/internal/storage"
)

const (
	defaultMaxDepth = 2
	defaultMaxPages = 50
	maxDepthLimit   = 10
	maxPagesLimit   = 1000
	maxURLLen       = 2048
)

// Canceler — то, что API требует от пула воркеров.
type Canceler interface {
	Cancel(id string) bool
}

type Server struct {
	store    *storage.Store
	queue    *queue.Queue
	canceler Canceler
	log      *slog.Logger
}

func NewServer(st *storage.Store, q *queue.Queue, c Canceler, log *slog.Logger) *Server {
	return &Server{store: st, queue: q, canceler: c, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /api/v1/jobs", s.createJob)
	mux.HandleFunc("GET /api/v1/jobs", s.listJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.getJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/results", s.getResults)
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", s.cancelJob)
	return s.recoverMW(s.logMW(mux))
}

// ---------- handlers ----------

type createJobRequest struct {
	URL        string `json:"url"`
	MaxDepth   *int   `json:"max_depth"`
	MaxPages   *int   `json:"max_pages"`
	SameDomain *bool  `json:"same_domain"`
}

// jobView добавляет к Job вычисляемый прогресс.
type jobView struct {
	model.Job
	ProgressPercent int `json:"progress_percent"`
}

func toView(j *model.Job) jobView {
	pct := 0
	switch {
	case j.Status == model.StatusDone:
		pct = 100
	case j.MaxPages > 0:
		pct = j.PagesCrawled * 100 / j.MaxPages
		if pct > 99 {
			pct = 99
		}
	}
	return jobView{Job: *j, ProgressPercent: pct}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createJobRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "body must contain a single JSON object")
		return
	}

	rawURL := strings.TrimSpace(req.URL)
	if err := validateURL(rawURL); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	job := &model.Job{URL: rawURL, MaxDepth: defaultMaxDepth, MaxPages: defaultMaxPages, SameDomain: true}
	if req.MaxDepth != nil {
		if *req.MaxDepth < 0 || *req.MaxDepth > maxDepthLimit {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("max_depth must be in [0, %d]", maxDepthLimit))
			return
		}
		job.MaxDepth = *req.MaxDepth
	}
	if req.MaxPages != nil {
		if *req.MaxPages < 1 || *req.MaxPages > maxPagesLimit {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("max_pages must be in [1, %d]", maxPagesLimit))
			return
		}
		job.MaxPages = *req.MaxPages
	}
	if req.SameDomain != nil {
		job.SameDomain = *req.SameDomain
	}

	if err := s.store.CreateJob(r.Context(), job); err != nil {
		s.log.Error("create job", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}
	// Асинхронная постановка в очередь. Если очередь полна — задание останется
	// в БД как pending, и диспетчер подберёт его позже.
	if err := s.queue.TryEnqueue(job.ID); err != nil {
		s.log.Warn("queue full, job will be picked up by dispatcher", "job", job.ID)
	}

	w.Header().Set("Location", "/api/v1/jobs/"+job.ID)
	writeJSON(w, http.StatusAccepted, toView(job))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, toView(job))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	if status != "" && !model.JobStatus(status).Valid() {
		writeError(w, http.StatusBadRequest, "unknown status filter")
		return
	}
	limit, offset, err := pagination(r, 20, 200)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	jobs, err := s.store.ListJobs(r.Context(), status, limit, offset)
	if err != nil {
		s.log.Error("list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}
	views := make([]jobView, 0, len(jobs))
	for i := range jobs {
		views = append(views, toView(&jobs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": views, "limit": limit, "offset": offset})
}

func (s *Server) getResults(w http.ResponseWriter, r *http.Request) {
	job, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	limit, offset, err := pagination(r, 50, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	pages, total, err := s.store.ListPages(r.Context(), job.ID, limit, offset)
	if err != nil {
		s.log.Error("list pages", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to load results")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"job_id": job.ID,
		"status": job.Status,
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  pages,
	})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	job, ok := s.loadJob(w, r)
	if !ok {
		return
	}
	switch job.Status {
	case model.StatusPending:
		canceled, err := s.store.CancelPending(r.Context(), job.ID)
		if err != nil {
			s.log.Error("cancel pending", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to cancel job")
			return
		}
		if !canceled { // воркер успел взять задание между проверкой и отменой
			s.canceler.Cancel(job.ID)
		}
	case model.StatusRunning:
		s.canceler.Cancel(job.ID)
	default:
		writeError(w, http.StatusConflict, "job is already "+string(job.Status))
		return
	}
	// Отмена выполняющегося задания асинхронна: статус сменится на canceled
	// через мгновение; отдаём актуальное состояние.
	if fresh, err := s.store.GetJob(r.Context(), job.ID); err == nil {
		job = fresh
	}
	writeJSON(w, http.StatusAccepted, toView(job))
}

// ---------- helpers ----------

func (s *Server) loadJob(w http.ResponseWriter, r *http.Request) (*model.Job, bool) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
		} else {
			s.log.Error("get job", "err", err)
			writeError(w, http.StatusInternalServerError, "failed to load job")
		}
		return nil, false
	}
	return job, true
}

func validateURL(raw string) error {
	if raw == "" {
		return errors.New("url is required")
	}
	if len(raw) > maxURLLen {
		return errors.New("url is too long")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("url is invalid")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("url scheme must be http or https")
	}
	if u.Hostname() == "" {
		return errors.New("url must contain a host")
	}
	return nil
}

func pagination(r *http.Request, defLimit, maxLimit int) (limit, offset int, err error) {
	limit, offset = defLimit, 0
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 || limit > maxLimit {
			return 0, 0, fmt.Errorf("limit must be in [1, %d]", maxLimit)
		}
	}
	if v := q.Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || offset < 0 {
			return 0, 0, errors.New("offset must be >= 0")
		}
	}
	return limit, offset, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ---------- middleware ----------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "dur", time.Since(began).String())
	})
}

func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in handler", "panic", rec)
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
