// Package crawler реализует обход сайта в ширину (BFS).
package crawler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/html"

	"webcrawler/internal/model"
)

type Config struct {
	Timeout      time.Duration // таймаут одного запроса
	Delay        time.Duration // пауза между запросами внутри задания
	MaxBody      int64         // лимит чтения тела страницы
	UserAgent    string
	AllowPrivate bool // разрешить localhost / частные сети (по умолчанию — запрещено, защита от SSRF)
}

type Options struct {
	URL        string
	MaxDepth   int
	MaxPages   int
	SameDomain bool
}

type Crawler struct {
	client *http.Client
	cfg    Config
}

func New(cfg Config) *Crawler {
	if cfg.UserAgent == "" {
		cfg.UserAgent = "AsyncWebCrawler/1.0"
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 2 << 20
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if !cfg.AllowPrivate {
		// Control вызывается после DNS-резолва прямо перед connect(),
		// поэтому защищает и от DNS rebinding, и от редиректов на внутренние адреса.
		dialer.Control = blockPrivate
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       30 * time.Second,
	}
	return &Crawler{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.Timeout,
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return errors.New("too many redirects")
				}
				return nil
			},
		},
	}
}

func blockPrivate(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("invalid ip %q", host)
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("address %s is not allowed", ip)
	}
	return nil
}

type item struct {
	u     *url.URL
	depth int
}

// Crawl обходит сайт и вызывает emit для каждой обработанной страницы.
// Ошибки отдельных страниц не прерывают обход (они попадают в Page.Error);
// ошибкой завершается только сбой стартовой страницы или отмена контекста.
func (c *Crawler) Crawl(ctx context.Context, opt Options, emit func(model.Page)) error {
	start, err := normalize(opt.URL)
	if err != nil {
		return fmt.Errorf("invalid start url: %w", err)
	}

	visited := map[string]struct{}{start.String(): {}}
	visitedCap := opt.MaxPages * 100
	queue := []item{{u: start, depth: 0}}
	crawled := 0

	for len(queue) > 0 && crawled < opt.MaxPages {
		if err := ctx.Err(); err != nil {
			return err
		}
		it := queue[0]
		queue = queue[1:]

		if crawled > 0 && c.cfg.Delay > 0 {
			select {
			case <-time.After(c.cfg.Delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		page, links := c.fetch(ctx, it.u, it.depth)
		if err := ctx.Err(); err != nil {
			return err // страницу, прерванную отменой, не сохраняем
		}
		crawled++
		emit(page)

		if page.Error != "" {
			if it.depth == 0 {
				return fmt.Errorf("start page failed: %s", page.Error)
			}
			continue
		}
		if it.depth >= opt.MaxDepth {
			continue
		}
		for _, l := range links {
			if opt.SameDomain && !strings.EqualFold(l.Hostname(), start.Hostname()) {
				continue
			}
			key := l.String()
			if _, seen := visited[key]; seen {
				continue
			}
			if len(visited) >= visitedCap {
				break
			}
			visited[key] = struct{}{}
			queue = append(queue, item{u: l, depth: it.depth + 1})
		}
	}
	return nil
}

func (c *Crawler) fetch(ctx context.Context, u *url.URL, depth int) (model.Page, []*url.URL) {
	began := time.Now()
	page := model.Page{URL: u.String(), Depth: depth, FetchedAt: began}
	finish := func() (model.Page, []*url.URL) {
		page.DurationMS = time.Since(began).Milliseconds()
		page.URL = clean(page.URL)
		page.Title = clean(page.Title)
		page.Error = clean(page.Error)
		return page, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		page.Error = err.Error()
		return finish()
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")

	resp, err := c.client.Do(req)
	if err != nil {
		page.Error = err.Error()
		return finish()
	}
	defer resp.Body.Close()

	page.StatusCode = resp.StatusCode
	page.ContentType = resp.Header.Get("Content-Type")

	if resp.StatusCode >= 400 {
		page.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return finish()
	}
	ct := strings.ToLower(page.ContentType)
	if !strings.Contains(ct, "text/html") && !strings.Contains(ct, "application/xhtml") {
		page.ContentLength = max(resp.ContentLength, 0)
		return finish()
	}

	cr := &countingReader{r: io.LimitReader(resp.Body, c.cfg.MaxBody)}
	title, links, perr := parseHTML(cr, resp.Request.URL)
	page.ContentLength = cr.n
	page.Title = title
	page.LinksCount = len(links)
	if perr != nil {
		page.Error = perr.Error()
		links = nil
	}
	page.DurationMS = time.Since(began).Milliseconds()
	page.URL = clean(page.URL)
	page.Title = clean(page.Title)
	page.Error = clean(page.Error)
	return page, links
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func parseHTML(r io.Reader, base *url.URL) (string, []*url.URL, error) {
	z := html.NewTokenizer(r)
	var (
		title   strings.Builder
		inTitle bool
		seen    = map[string]struct{}{}
		links   []*url.URL
	)
	for {
		switch z.Next() {
		case html.ErrorToken:
			err := z.Err()
			if errors.Is(err, io.EOF) {
				return tidyTitle(title.String()), links, nil
			}
			return tidyTitle(title.String()), links, err
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			switch string(name) {
			case "title":
				inTitle = true
			case "a":
				for hasAttr {
					var k, v []byte
					k, v, hasAttr = z.TagAttr()
					if string(k) != "href" {
						continue
					}
					if l := resolve(base, string(v)); l != nil {
						key := l.String()
						if _, ok := seen[key]; !ok {
							seen[key] = struct{}{}
							links = append(links, l)
						}
					}
				}
			}
		case html.EndTagToken:
			if name, _ := z.TagName(); string(name) == "title" {
				inTitle = false
			}
		case html.TextToken:
			if inTitle && title.Len() < 1000 {
				title.Write(z.Text())
			}
		}
	}
}

func tidyTitle(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 300 {
		s = string(r[:300])
	}
	return s
}

func resolve(base *url.URL, href string) *url.URL {
	href = strings.TrimSpace(href)
	if href == "" {
		return nil
	}
	ref, err := url.Parse(href)
	if err != nil {
		return nil
	}
	abs, err := normalize(base.ResolveReference(ref).String())
	if err != nil {
		return nil
	}
	return abs
}

// normalize принимает только http(s) с непустым хостом и убирает fragment.
func normalize(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return nil, errors.New("empty host")
	}
	u.Fragment = ""
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	if u.Path == "" {
		u.Path = "/"
	}
	return u, nil
}

// clean убирает NUL-байты и невалидный UTF-8 — PostgreSQL их не принимает в TEXT.
func clean(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, ""), "\x00", "")
}
