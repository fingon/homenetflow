WATCHMAN_MAKE ?= watchman-make
UI_PID_FILE ?= /tmp/homenetflow-ui-watch.pid
UI_LOG_FILE ?= /tmp/homenetflow-ui-watch.log
UI_WATCH_PATTERNS = '**/*.go' 'internal/parquetui/static/*.js' 'internal/parquetui/static/*.css' 'go.mod' 'go.sum' 'Makefile'
UI_RUN_ARGS = ./cmd/parquetflowui data/parquet -v
UI_STOP_WAIT_ATTEMPTS ?= 50
UI_STOP_WAIT_SLEEP_SEC ?= 0.1

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
	go run $(UI_RUN_ARGS)

.PHONY: ui-run
ui-run:
	@set -eu; \
	nohup go run $(UI_RUN_ARGS) --dev >$(UI_LOG_FILE) 2>&1 & \
	echo $$! >$(UI_PID_FILE)

.PHONY: ui-stop
ui-stop:
	@set -eu; \
	if [ -f "$(UI_PID_FILE)" ]; then \
		pid=$$(cat "$(UI_PID_FILE)"); \
		if kill -0 "$$pid" 2>/dev/null; then \
			kill "$$pid"; \
			attempt=0; \
			while kill -0 "$$pid" 2>/dev/null; do \
				attempt=$$((attempt + 1)); \
				if [ "$$attempt" -ge "$(UI_STOP_WAIT_ATTEMPTS)" ]; then \
					kill -KILL "$$pid"; \
				fi; \
				sleep "$(UI_STOP_WAIT_SLEEP_SEC)"; \
			done; \
		fi; \
		rm -f "$(UI_PID_FILE)"; \
	fi

.PHONY: ui-restart
ui-restart: ui-stop ui-run

.PHONY: ui-watch
ui-watch: ui-restart
	$(WATCHMAN_MAKE) -p $(UI_WATCH_PATTERNS) --make "$(MAKE)" -t ui-restart

.PHONY: check
check: lint test build
