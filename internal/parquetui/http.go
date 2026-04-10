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
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/kong"
)

//go:embed static/*
var staticFiles embed.FS

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
	ctx, cancel := context.WithCancel(context.Background())
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

	slog.Info("server starting", "url", fmt.Sprintf("http://localhost:%d", cfg.Port), "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listen and serve: %w", err)
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
