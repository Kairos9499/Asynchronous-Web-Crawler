// Командный клиент для REST API краулера.
//
//	client create [-depth N] [-pages N] [-same-domain=true] [-wait] <url>
//	client status <id>
//	client wait <id>
//	client results [-all] [-limit N] [-offset N] [-json] <id>
//	client list [-status pending|running|done|failed|canceled]
//	client cancel <id>
//
// Адрес сервера: флаг -server или переменная CRAWLER_URL (по умолчанию http://localhost:8080).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type job struct {
	ID              string `json:"id"`
	URL             string `json:"url"`
	MaxDepth        int    `json:"max_depth"`
	MaxPages        int    `json:"max_pages"`
	SameDomain      bool   `json:"same_domain"`
	Status          string `json:"status"`
	Error           string `json:"error"`
	PagesCrawled    int    `json:"pages_crawled"`
	PagesFailed     int    `json:"pages_failed"`
	ProgressPercent int    `json:"progress_percent"`
	CreatedAt       string `json:"created_at"`
	StartedAt       string `json:"started_at"`
	FinishedAt      string `json:"finished_at"`
}

type page struct {
	URL        string `json:"url"`
	Depth      int    `json:"depth"`
	StatusCode int    `json:"status_code"`
	Title      string `json:"title"`
	LinksCount int    `json:"links_count"`
	Error      string `json:"error"`
	DurationMS int64  `json:"duration_ms"`
}

type client struct {
	base string
	http *http.Client
}

func (c *client) do(method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, strings.TrimRight(c.base, "/")+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func defaultServer() string {
	if v := os.Getenv("CRAWLER_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func newFlags(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	server := fs.String("server", defaultServer(), "адрес сервера")
	return fs, server
}

func newClient(server string) *client {
	return &client{base: server, http: &http.Client{Timeout: 30 * time.Second}}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprint(os.Stderr, `Использование:
  client create [-depth N] [-pages N] [-same-domain=true] [-wait] <url>
  client status <id>
  client wait <id>
  client results [-all] [-limit N] [-offset N] [-json] <id>
  client list [-status pending|running|done|failed|canceled]
  client cancel <id>

Флаги указываются ДО позиционных аргументов. Адрес сервера: -server или CRAWLER_URL.
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "create":
		cmdCreate(args)
	case "status":
		cmdStatus(args)
	case "wait":
		cmdWait(args)
	case "results":
		cmdResults(args)
	case "list":
		cmdList(args)
	case "cancel":
		cmdCancel(args)
	default:
		usage()
	}
}

func cmdCreate(args []string) {
	fs, server := newFlags("create")
	depth := fs.Int("depth", 2, "максимальная глубина обхода")
	pages := fs.Int("pages", 50, "максимум страниц")
	same := fs.Bool("same-domain", true, "не выходить за пределы домена")
	wait := fs.Bool("wait", false, "дождаться завершения")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	c := newClient(*server)
	var j job
	err := c.do("POST", "/api/v1/jobs", map[string]any{
		"url": fs.Arg(0), "max_depth": *depth, "max_pages": *pages, "same_domain": *same,
	}, &j)
	if err != nil {
		fatal(err)
	}
	printJob(j)
	if *wait {
		waitJob(c, j.ID)
	}
}

func cmdStatus(args []string) {
	fs, server := newFlags("status")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	var j job
	if err := newClient(*server).do("GET", "/api/v1/jobs/"+fs.Arg(0), nil, &j); err != nil {
		fatal(err)
	}
	printJob(j)
}

func cmdWait(args []string) {
	fs, server := newFlags("wait")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	waitJob(newClient(*server), fs.Arg(0))
}

func cmdCancel(args []string) {
	fs, server := newFlags("cancel")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	var j job
	if err := newClient(*server).do("POST", "/api/v1/jobs/"+fs.Arg(0)+"/cancel", nil, &j); err != nil {
		fatal(err)
	}
	printJob(j)
}

func cmdList(args []string) {
	fs, server := newFlags("list")
	status := fs.String("status", "", "фильтр по статусу")
	_ = fs.Parse(args)
	path := "/api/v1/jobs"
	if *status != "" {
		path += "?status=" + *status
	}
	var resp struct {
		Items []job `json:"items"`
	}
	if err := newClient(*server).do("GET", path, nil, &resp); err != nil {
		fatal(err)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATUS\tPAGES\tURL")
	for _, j := range resp.Items {
		fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%s\n", j.ID, j.Status, j.PagesCrawled, j.MaxPages, j.URL)
	}
	tw.Flush()
}

func cmdResults(args []string) {
	fs, server := newFlags("results")
	limit := fs.Int("limit", 50, "размер страницы (максимум 500)")
	offset := fs.Int("offset", 0, "смещение")
	all := fs.Bool("all", false, "получить все результаты (запрашивает порциями по 500)")
	asJSON := fs.Bool("json", false, "вывести JSON")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	c := newClient(*server)
	id := fs.Arg(0)

	var items []json.RawMessage
	total := 0
	fetch := func(lim, off int) (int, int) {
		var resp struct {
			Total int               `json:"total"`
			Items []json.RawMessage `json:"items"`
		}
		path := fmt.Sprintf("/api/v1/jobs/%s/results?limit=%d&offset=%d", id, lim, off)
		if err := c.do("GET", path, nil, &resp); err != nil {
			fatal(err)
		}
		items = append(items, resp.Items...)
		return resp.Total, len(resp.Items)
	}

	if *all {
		const step = 500
		for off := 0; ; off += step {
			var got int
			total, got = fetch(step, off)
			if got == 0 || off+got >= total {
				break
			}
		}
	} else {
		total, _ = fetch(*limit, *offset)
	}

	if *asJSON {
		out, _ := json.MarshalIndent(map[string]any{"total": total, "items": items}, "", "  ")
		fmt.Println(string(out))
		return
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "DEPTH\tHTTP\tLINKS\tMS\tURL\tTITLE / ERROR")
	for _, raw := range items {
		var p page
		if err := json.Unmarshal(raw, &p); err != nil {
			continue
		}
		info := p.Title
		if p.Error != "" {
			info = "ERROR: " + p.Error
		}
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%s\t%s\n", p.Depth, p.StatusCode, p.LinksCount, p.DurationMS, p.URL, info)
	}
	tw.Flush()
	fmt.Printf("\nпоказано %d из %d\n", len(items), total)
}

func printJob(j job) {
	fmt.Printf("ID:        %s\n", j.ID)
	fmt.Printf("URL:       %s\n", j.URL)
	fmt.Printf("Статус:    %s (%d%%)\n", j.Status, j.ProgressPercent)
	fmt.Printf("Страниц:   %d из max %d (ошибок: %d)\n", j.PagesCrawled, j.MaxPages, j.PagesFailed)
	fmt.Printf("Глубина:   %d, same_domain=%v\n", j.MaxDepth, j.SameDomain)
	if j.Error != "" {
		fmt.Printf("Ошибка:    %s\n", j.Error)
	}
	fmt.Printf("Создано:   %s\n", j.CreatedAt)
	if j.StartedAt != "" {
		fmt.Printf("Начато:    %s\n", j.StartedAt)
	}
	if j.FinishedAt != "" {
		fmt.Printf("Завершено: %s\n", j.FinishedAt)
	}
}

func waitJob(c *client, id string) {
	for {
		var j job
		if err := c.do("GET", "/api/v1/jobs/"+id, nil, &j); err != nil {
			fatal(err)
		}
		fmt.Printf("\r%-9s %3d%%  страниц: %d  ", j.Status, j.ProgressPercent, j.PagesCrawled)
		switch j.Status {
		case "done":
			fmt.Println("\nготово")
			return
		case "failed":
			fmt.Printf("\nошибка: %s\n", j.Error)
			os.Exit(1)
		case "canceled":
			fmt.Println("\nотменено")
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}
}
