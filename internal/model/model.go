// Package model содержит доменные типы, общие для всех слоёв приложения.
package model

import "time"

type JobStatus string

const (
	StatusPending  JobStatus = "pending"  // создано, ждёт воркера
	StatusRunning  JobStatus = "running"  // обход выполняется
	StatusDone     JobStatus = "done"     // успешно завершено
	StatusFailed   JobStatus = "failed"   // завершено с ошибкой
	StatusCanceled JobStatus = "canceled" // отменено пользователем
)

func (s JobStatus) Valid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusDone, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

func (s JobStatus) Terminal() bool {
	return s == StatusDone || s == StatusFailed || s == StatusCanceled
}

// Job — задание на обход сайта.
type Job struct {
	ID           string     `json:"id"`
	URL          string     `json:"url"`
	MaxDepth     int        `json:"max_depth"`
	MaxPages     int        `json:"max_pages"`
	SameDomain   bool       `json:"same_domain"`
	Status       JobStatus  `json:"status"`
	Error        string     `json:"error,omitempty"`
	PagesCrawled int        `json:"pages_crawled"`
	PagesFailed  int        `json:"pages_failed"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

// Page — результат обработки одной страницы.
type Page struct {
	ID            int64     `json:"id"`
	JobID         string    `json:"job_id"`
	URL           string    `json:"url"`
	Depth         int       `json:"depth"`
	StatusCode    int       `json:"status_code"`
	Title         string    `json:"title"`
	ContentType   string    `json:"content_type"`
	ContentLength int64     `json:"content_length"`
	LinksCount    int       `json:"links_count"`
	Error         string    `json:"error,omitempty"`
	DurationMS    int64     `json:"duration_ms"`
	FetchedAt     time.Time `json:"fetched_at"`
}
