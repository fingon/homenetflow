package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/fingon/homenetflow/internal/app"
	"github.com/fingon/homenetflow/internal/model"
	"github.com/fingon/homenetflow/internal/nfdumpparser"
)

type cli struct {
	DstPath     string `help:"Flat output directory for parquet files." name:"dst" required:""`
	Now         string `help:"Reference time for month/day/hour bucketing, in RFC3339." name:"now"`
	Parallelism int    `help:"Parser workers per parquet output. 0 auto-tunes." name:"parallelism" short:"j"`
	SrcPath     string `help:"Root input directory containing YYYY/MM/DD/HH/nfcapd.* files." name:"src" required:""`
	Verbose     bool   `help:"Enable verbose logging." name:"v" short:"v"`
}

func main() {
	var commandLine cli
	parser := kong.Must(&commandLine,
		kong.Name("nfdump2parquet"),
		kong.Description("Convert hierarchical nfdump inputs into flat parquet outputs."),
	)
	_, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	logLevel := slog.LevelInfo
	if commandLine.Verbose {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	now := time.Now().UTC()
	if commandLine.Now != "" {
		parsedTime, err := time.Parse(time.RFC3339, commandLine.Now)
		if err != nil {
			slog.Error("invalid --now value", "error", err, "value", commandLine.Now)
			os.Exit(1)
		}

		now = parsedTime.UTC()
	}

	if err := app.Run(app.Config{
		DstPath:     commandLine.DstPath,
		Now:         now,
		Parallelism: commandLine.Parallelism,
		SrcPath:     commandLine.SrcPath,
	}, func() model.FlowParser {
		return nfdumpparser.Parser{}
	}); err != nil {
		slog.Error("nfdump2parquet failed", "error", err)
		os.Exit(1)
	}
}
