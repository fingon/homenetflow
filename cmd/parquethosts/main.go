package main

import (
	"log/slog"
	"os"

	"github.com/alecthomas/kong"
	"github.com/fingon/homenetflow/internal/enrich"
)

type cli struct {
	DstPath        string `help:"Flat output directory for enriched parquet files." name:"dst" required:""`
	SkipDNSLookups bool   `help:"Skip live PTR lookups and use only dnsmasq logs plus existing reverse DNS cache entries." name:"skip-dns-lookups"`
	SrcLogPath     string `help:"Directory containing dnsmasq YYYY-MM-DD.jsonl files." name:"src-log" required:""`
	SrcParquetPath string `help:"Flat input directory containing nfcap_*.parquet files." name:"src-parquet" required:""`
	Verbose        bool   `help:"Enable verbose logging." name:"v" short:"v"`
}

func main() {
	var commandLine cli
	parser := kong.Must(&commandLine,
		kong.Name("parquethosts"),
		kong.Description("Enrich flat parquet flow files with host, 2LD, and TLD fields."),
	)
	_, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	logLevel := slog.LevelInfo
	if commandLine.Verbose {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	progress := newEnrichProgress(os.Stderr)

	if err := enrich.Run(enrich.Config{
		DstPath:        commandLine.DstPath,
		Progress:       progress.callback,
		SkipDNSLookups: commandLine.SkipDNSLookups,
		SrcLogPath:     commandLine.SrcLogPath,
		SrcParquetPath: commandLine.SrcParquetPath,
	}); err != nil {
		slog.Error("parquethosts failed", "error", err)
		os.Exit(1)
	}
}
