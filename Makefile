.PHONY: all
all: test install

.PHONY: install
install:
	go install ./cmd/...

.PHONY: lint
lint:
	go tool golangci-lint run --fix

.PHONY: test
test:
	go test ./...

.PHONY: build
build:
	go build ./...

.PHONY: ui
ui:
	go run ./cmd/parquetflowui data/parquet -v

.PHONY: check
check: lint test build
