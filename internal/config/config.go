// Package config загружает настройки сервера из переменных окружения.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr          string
	DatabaseURL       string
	Workers           int
	QueueSize         int
	PollInterval      time.Duration // как часто подбирать pending-задания из БД
	RequestTimeout    time.Duration // таймаут одного HTTP-запроса краулера
	CrawlDelay        time.Duration // пауза между запросами внутри задания
	JobTimeout        time.Duration // максимальная длительность одного задания
	MaxBodyBytes      int64         // максимум байт, читаемых с одной страницы
	AllowPrivateHosts bool          // разрешить обход localhost/частных сетей
}

func Load() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:          str("HTTP_ADDR", ":8080"),
		DatabaseURL:       str("DATABASE_URL", "postgres://crawler:crawler@localhost:5432/crawler?sslmode=disable"),
		Workers:           integer("WORKERS", 4, &errs),
		QueueSize:         integer("QUEUE_SIZE", 100, &errs),
		PollInterval:      duration("POLL_INTERVAL", 5*time.Second, &errs),
		RequestTimeout:    duration("REQUEST_TIMEOUT", 15*time.Second, &errs),
		CrawlDelay:        duration("CRAWL_DELAY", 200*time.Millisecond, &errs),
		JobTimeout:        duration("JOB_TIMEOUT", 10*time.Minute, &errs),
		MaxBodyBytes:      int64(integer("MAX_BODY_BYTES", 2<<20, &errs)),
		AllowPrivateHosts: boolean("ALLOW_PRIVATE_HOSTS", false, &errs),
	}
	if c.Workers < 1 {
		errs = append(errs, errors.New("WORKERS must be >= 1"))
	}
	if c.QueueSize < 1 {
		errs = append(errs, errors.New("QUEUE_SIZE must be >= 1"))
	}
	return c, errors.Join(errs...)
}

func str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func integer(key string, def int, errs *[]error) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func duration(key string, def time.Duration, errs *[]error) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func boolean(key string, def bool, errs *[]error) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}
