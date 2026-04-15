package parquetui

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
)

//go:embed static/*
var staticFiles embed.FS

const serverShutdownTimeout = 5 * time.Second

type App struct {
	devMode         bool
	devSessionToken string
	service         *Service
}

func Run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	configureLogging(cfg.Verbose)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	service, err := NewService(ctx, cfg.SrcParquetPath, cfg.ReloadInterval)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	defer service.Close()
	service.StartMonitor(ctx)

	app := &App{
		devMode:         cfg.Dev,
		devSessionToken: newDevSessionToken(),
		service:         service,
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	guard, err := acquireProcessGuard(cfg.PIDFile)
	if err != nil {
		return fmt.Errorf("acquire process guard: %w", err)
	}
	if guard != nil {
		defer func() {
			if err := guard.Close(); err != nil {
				slog.Warn("release process guard failed", "pid_file", cfg.PIDFile, "err", err)
			}
		}()
		if cfg.ReplaceRunning {
			if err := guard.replaceRunningProcess(); err != nil {
				return fmt.Errorf("replace running process: %w", err)
			}
		}
	}

	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	if err := writePIDFile(cfg.PIDFile, os.Getpid()); err != nil {
		return fmt.Errorf("write pid file: %w", err)
	}
	defer func() {
		if err := removePIDFileIfCurrent(cfg.PIDFile, os.Getpid()); err != nil {
			slog.Warn("remove pid file failed", "pid_file", cfg.PIDFile, "err", err)
		}
	}()

	if guard != nil {
		if err := guard.Close(); err != nil {
			return fmt.Errorf("release process guard: %w", err)
		}
		guard = nil
	}

	shutdownCompleteChannel := make(chan struct{})
	go func() {
		defer close(shutdownCompleteChannel)
		<-ctx.Done()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("server shutdown failed", "err", err)
		}
	}()

	slog.Info("server starting", "url", fmt.Sprintf("http://localhost:%d", cfg.Port), "address", server.Addr)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	if ctx.Err() != nil {
		<-shutdownCompleteChannel
	}
	return nil
}

func parseConfig(args []string) (Config, error) {
	var cfg Config
	parser, err := kong.New(
		&cfg,
		kong.Name("parquetflowui"),
		kong.Description("Web UI for browsing enriched netflow parquet files."),
		kong.UsageOnError(),
	)
	if err != nil {
		return cfg, fmt.Errorf("create parser: %w", err)
	}
	if _, err := parser.Parse(args); err != nil {
		return cfg, fmt.Errorf("parse args: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

func configureLogging(verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/version", a.handleVersion)
	mux.HandleFunc("/flows", a.handleFlows)
	mux.HandleFunc("/", a.handleIndex)
	return requestLogger(mux)
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Debug("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	dashboard, err := a.service.Dashboard(r.Context(), ParseQueryState(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isHTMXRequest(r) {
		if err := AppShell(dashboard).Render(w); err != nil {
			slog.Error("render app shell failed", "err", err)
			http.Error(w, "failed rendering app shell", http.StatusInternalServerError)
		}
		return
	}

	if err := Index(dashboard, a.devMode, a.devSessionToken).Render(w); err != nil {
		slog.Error("render index failed", "err", err)
		http.Error(w, "failed rendering index", http.StatusInternalServerError)
	}
}

func (a *App) handleFlows(w http.ResponseWriter, r *http.Request) {
	flowQuery, err := ParseFlowQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	flows, err := a.service.FlowDetails(r.Context(), flowQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isHTMXRequest(r) {
		if err := FlowDetailShell(flows).Render(w); err != nil {
			slog.Error("render flow detail shell failed", "err", err)
			http.Error(w, "failed rendering flow detail shell", http.StatusInternalServerError)
		}
		return
	}

	if err := FlowDetailIndex(flows, a.devMode, a.devSessionToken).Render(w); err != nil {
		slog.Error("render flow detail index failed", "err", err)
		http.Error(w, "failed rendering flow detail index", http.StatusInternalServerError)
	}
}

func isHTMXRequest(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

func (a *App) handleVersion(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if _, err := w.Write([]byte(a.devSessionToken)); err != nil {
		slog.Warn("write version response failed", "err", err)
	}
}

func newDevSessionToken() string {
	const devSessionTokenBytes = 16

	buffer := make([]byte, devSessionTokenBytes)
	if _, err := rand.Read(buffer); err != nil {
		slog.Warn("generate dev session token failed", "err", err)
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buffer)
}
