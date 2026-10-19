# syntax=docker/dockerfile:1

# ---------- base: исходники + зависимости ----------
FROM golang:1.22-alpine AS base
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod go mod tidy

# ---------- test: docker build --target test . ----------
FROM base AS test
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go test ./...

# ---------- build ----------
FROM base AS build
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

# ---------- runtime (по умолчанию) ----------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 app
COPY --from=build /out/server /out/client /usr/local/bin/
USER app
EXPOSE 8080
ENTRYPOINT ["server"]
