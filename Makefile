WATCHMAN_MAKE ?= watchman-make
UI_PID_FILE ?= /tmp/homenetflow-ui-watch.pid
UI_LOG_FILE ?= /tmp/homenetflow-ui-watch.log
UI_WATCH_PATTERNS = '**/*.go' 'internal/parquetui/static/*.js' 'internal/parquetui/static/*.css' 'go.mod' 'go.sum' 'Makefile'
UI_RUN_ARGS = ./cmd/parquetflowui data/parquet -v

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
	@set -eu; \
	nohup go run $(UI_RUN_ARGS) --dev --pid-file "$(UI_PID_FILE)" --replace-running >$(UI_LOG_FILE) 2>&1 &

.PHONY: ui-watch
ui-watch: ui
	$(WATCHMAN_MAKE) -p $(UI_WATCH_PATTERNS) --make "$(MAKE)" -t ui

.PHONY: check
check: lint test build
