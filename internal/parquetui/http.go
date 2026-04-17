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
	"strings"
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
	mux.HandleFunc("/ignore-rules", a.handleIgnoreRules)
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
		if errors.Is(err, errEntityActionsDisabled) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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

func (a *App) handleIgnoreRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pageData := a.ignoreRulePageData(r, nil, "")
		a.renderIgnoreRules(w, r, pageData)
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, "invalid form data", http.StatusBadRequest)
			return
		}
		action := strings.TrimSpace(r.PostForm.Get("action"))
		switch action {
		case "delete":
			if err := a.service.DeleteIgnoreRule(strings.TrimSpace(r.PostForm.Get("rule_id"))); err != nil {
				pageData := a.ignoreRulePageData(r, nil, err.Error())
				a.renderIgnoreRules(w, r, pageData)
				return
			}
			pageData := a.ignoreRulePageData(r, nil, "")
			a.renderIgnoreRules(w, r, pageData)
		default:
			rule, err := newIgnoreRuleFromForm(r.PostForm, time.Now().UTC())
			if err != nil {
				pageData := a.ignoreRulePageData(r, &rule, err.Error())
				a.renderIgnoreRules(w, r, pageData)
				return
			}
			if err := a.service.SaveIgnoreRule(rule); err != nil {
				pageData := a.ignoreRulePageData(r, &rule, err.Error())
				a.renderIgnoreRules(w, r, pageData)
				return
			}
			pageData := a.ignoreRulePageData(r, nil, "")
			a.renderIgnoreRules(w, r, pageData)
		}
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func isHTMXRequest(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

func (a *App) renderIgnoreRules(w http.ResponseWriter, r *http.Request, pageData IgnoreRulePageData) {
	if isHTMXRequest(r) {
		if err := IgnoreRulesShell(pageData).Render(w); err != nil {
			slog.Error("render ignore rules shell failed", "err", err)
			http.Error(w, "failed rendering ignore rules shell", http.StatusInternalServerError)
		}
		return
	}
	if err := IgnoreRulesIndex(pageData, a.devMode, a.devSessionToken).Render(w); err != nil {
		slog.Error("render ignore rules index failed", "err", err)
		http.Error(w, "failed rendering ignore rules index", http.StatusInternalServerError)
	}
}

func (a *App) ignoreRulePageData(r *http.Request, editRule *IgnoreRule, errorMessage string) IgnoreRulePageData {
	values := r.URL.Query()
	if r.Method == http.MethodPost {
		values = r.PostForm
	}

	returnURL := strings.TrimSpace(values.Get("return_to"))
	if returnURL == "" {
		returnURL = "/"
	}
	returnLabel := strings.TrimSpace(values.Get("return_label"))
	if returnLabel == "" {
		returnLabel = "Back"
	}

	rules := a.service.ignoreRulesSnapshot()
	var selectedRule IgnoreRule
	switch {
	case editRule != nil:
		selectedRule = normalizeIgnoreRule(*editRule)
	case strings.TrimSpace(values.Get("rule_id")) != "":
		if existingRule, ok := ignoreRuleByID(rules, strings.TrimSpace(values.Get("rule_id"))); ok {
			selectedRule = existingRule
			if values.Has("rule_name") || values.Has("rule_any_entity") || values.Has("rule_source_entity") || values.Has("rule_destination_entity") {
				selectedRule = prefilledIgnoreRule(values)
				selectedRule.ID = existingRule.ID
				selectedRule.CreatedAtNs = existingRule.CreatedAtNs
			}
		} else {
			selectedRule = prefilledIgnoreRule(values)
		}
	default:
		selectedRule = prefilledIgnoreRule(values)
	}

	return IgnoreRulePageData{
		EditRule:     selectedRule,
		ErrorMessage: errorMessage,
		ReturnLabel:  returnLabel,
		ReturnURL:    returnURL,
		Rules:        rules,
		ValidationHints: []string{
			"Any entity matches either side using exact host, 2LD, TLD, or IP values.",
			"CIDR matching applies to source and destination IP fields when DuckDB inet support is available.",
			"Protocol and port fields apply to flow traffic, not DNS lookup summaries.",
		},
	}
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
