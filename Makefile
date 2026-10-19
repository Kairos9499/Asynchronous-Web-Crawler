.PHONY: up down logs test test-docker test-race tidy build run
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f server
# тесты внутри контейнера (Go на хосте не нужен)
test-docker:
	docker build --target test .

# --- локальная разработка (нужен Go на хосте) ---
tidy:
	go mod tidy
build: tidy
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client
test: tidy
	go test ./...
# детектор гонок требует cgo и gcc: CGO_ENABLED=1
test-race: tidy
	CGO_ENABLED=1 go test -race ./...
run: build
	./bin/server
