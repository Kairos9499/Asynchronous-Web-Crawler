# Асинхронный Web Crawler

Клиент-серверное приложение: сервер принимает задания на обход сайтов через REST API,
ставит их в асинхронную очередь, а пул воркеров (goroutines) выполняет обход в фоне.
Задания и результаты хранятся в PostgreSQL, статус и прогресс доступны через API.

## Архитектура

![Web Crawler (1)](D:\Download\Web Crawler (1).jpg)

## Структура

```
cmd/server      — точка входа сервера
cmd/client      — CLI-клиент
internal/api    — REST-хендлеры и middleware
internal/queue  — асинхронная очередь
internal/worker — пул воркеров и диспетчер
internal/crawler— BFS-краулер, парсинг HTML
internal/storage— PostgreSQL (pgx), миграции
internal/config — конфигурация из env
internal/model  — доменные типы
```

Жизненный цикл задания: `pending → running → done | failed | canceled`.

Ключевые решения:

- **Очередь** — буферизованный канал + защита от дублей. Если очередь переполнена, задание
  остаётся в БД как `pending`, а **диспетчер** раз в `POLL_INTERVAL` подбирает такие задания.
  Поэтому очередь переживает рестарт сервера: на старте `running → pending`.
- **Захват задания** — атомарный `UPDATE ... WHERE status='pending' RETURNING`, два воркера
  не возьмут одно задание.
- **Прогресс** — каждая страница сохраняется сразу, счётчики `pages_crawled / pages_failed`
  обновляются тем же SQL-запросом, поэтому `GET /jobs/{id}` показывает живой прогресс.
- **Отмена** — `POST /jobs/{id}/cancel` (pending отменяется в БД, running — через `context`).
- **Защита от SSRF** — по умолчанию краулер не ходит на loopback/частные/link-local адреса.
  Для локальных экспериментов: `ALLOW_PRIVATE_HOSTS=true`.
- **Краулер**: BFS, нормализация URL, дедупликация, лимиты глубины/страниц/размера тела,
  таймауты запроса и всего задания, пауза между запросами.

## Запуск

### Полностью в контейнерах

Cтек: PostgreSQL, сервер и CLI-клиент.

- `	docker compose up --build -d` - запуск сервера и БД
- `docker compose ps`  - проверка работы сервисов
- `docker compose run --rm client create -depth 1 -pages 20 -wait https://go.dev` - запуск краулера с глубиной 1, количеством страниц 20, на URL `https://go.dev``
- `docker compose run --rm client list` - выводит список заданий
- `docker compose run --rm client status <id>` - выводит статус задания
- `docker compose run --rm client results -all <id>` - выводит все результаты выполнения задания с указанным id
- `docker compose run --rm client cancel <id>` - отмена задания по id

**Остальное:**

- `docker compose logs -f server` - логи сервера

- `docker compose down` - остановить работу сервера и БД (данные сохраняются в томе pgdata)

- `docker compose down -v` - остановить работу сервера и БД и удалить данные

Настройки (порт, число воркеров, пароль БД) меняются через `.env`
(шаблон — `.env.example`). Порт PostgreSQL наружу не публикуется; для отладки
раскомментируйте `ports` у сервиса `db`. 

Примечание: внутри контейнера `localhost` — это сам контейнер, поэтому обход сайтов
на хосте (`localhost`, `host.docker.internal`) блокируется защитой от SSRF; для таких
экспериментов задайте `ALLOW_PRIVATE_HOSTS=true` в `.env`.

## Конфигурация (переменные окружения)

| Переменная            | По умолчанию | Описание                                      |
|-----------------------|--------------|-----------------------------------------------|
| `HTTP_ADDR`           | `:8080`      | адрес API                                     |
| `DATABASE_URL`        | `postgres://crawler:crawler@localhost:5432/crawler?sslmode=disable` | DSN PostgreSQL |
| `WORKERS`             | `4`          | число воркеров                                |
| `QUEUE_SIZE`          | `100`        | ёмкость очереди                               |
| `POLL_INTERVAL`       | `5s`         | период диспетчера                             |
| `REQUEST_TIMEOUT`     | `15s`        | таймаут одного HTTP-запроса                   |
| `CRAWL_DELAY`         | `200ms`      | пауза между запросами внутри задания          |
| `JOB_TIMEOUT`         | `10m`        | максимум времени на задание                   |
| `MAX_BODY_BYTES`      | `2097152`    | сколько байт читать с одной страницы          |
| `ALLOW_PRIVATE_HOSTS` | `false`      | разрешить обход localhost / частных сетей     |

## REST API

| Метод | Путь                          | Описание                              |
|-------|-------------------------------|---------------------------------------|
| POST  | `/api/v1/jobs`                | создать задание (202 Accepted)        |
| GET   | `/api/v1/jobs`                | список (`?status=&limit=&offset=`)    |
| GET   | `/api/v1/jobs/{id}`           | статус и прогресс                     |
| GET   | `/api/v1/jobs/{id}/results`   | страницы (`?limit=&offset=`)          |
| POST  | `/api/v1/jobs/{id}/cancel`    | отменить задание                      |
| GET   | `/healthz`                    | проверка живости и БД                 |

Создание задания (`max_depth` 0–10, по умолчанию 2; `max_pages` 1–1000, по умолчанию 50;
`same_domain` по умолчанию `true`):

```bash
curl -s -X POST localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://go.dev","max_depth":1,"max_pages":20}'
```

```json
{
  "id": "6f1c…",
  "url": "https://go.dev",
  "max_depth": 1, "max_pages": 20, "same_domain": true,
  "status": "pending",
  "pages_crawled": 0, "pages_failed": 0,
  "progress_percent": 0,
  "created_at": "2025-01-01T12:00:00Z"
}
```

Статус и результаты:

```bash
curl -s localhost:8080/api/v1/jobs/<id>
curl -s 'localhost:8080/api/v1/jobs/<id>/results?limit=10'
curl -s -X POST localhost:8080/api/v1/jobs/<id>/cancel
```

Ошибки возвращаются как `{"error": "..."}` с кодами 400 / 404 / 409 / 500.

