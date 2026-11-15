TEST_DATABASE_URL ?= postgres://pgqueue:pgqueue@localhost:5432/pgqueue?sslmode=disable

.PHONY: build test test-integration cover lint up down e2e bench

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -tags integration ./...

cover:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -tags integration -coverprofile=coverage.out ./ ./internal/...
	go tool cover -func=coverage.out | tail -1

lint:
	go vet ./... && go vet -tags integration ./...
	test -z "$$(gofmt -l .)"

up:
	docker compose up --build -d

down:
	docker compose down -v

e2e:
	./scripts/e2e.sh

bench:
	go run ./cmd/bench -dsn "$(TEST_DATABASE_URL)" -jobs 20000 -workers 4 -c 16
