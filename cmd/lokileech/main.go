package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/fingon/homenetflow/internal/lokileech"
)

type cli struct {
	Addr             string        `default:"https://fw.fingon.iki.fi:3100" env:"LOKI_ADDR" help:"Loki server address." name:"addr"`
	Batch            int           `default:"5000" help:"Loki query batch size." name:"batch"`
	Days             int           `default:"80" help:"Number of daily files to fetch." name:"days"`
	DstPath          string        `default:"." help:"Directory for YYYY-MM-DD.jsonl outputs." name:"dst"`
	ParallelDuration time.Duration `default:"15m" help:"Duration of each parallel Loki query range." name:"parallel-duration"`
	ParallelWorkers  int           `default:"10" help:"Maximum parallel Loki query workers per day." name:"parallel-workers"`
	Query            string        `default:"{source=~\"dnsmasq|ip_neighbour\"}" help:"LogQL query to fetch." name:"query"`
	Verbose          bool          `help:"Enable verbose logging." name:"v" short:"v"`
	AlsoToday        bool          `help:"Delete the newest daily output and include today's logs." name:"also-today"`
}

func main() {
	var commandLine cli
	parser := kong.Must(&commandLine,
		kong.Name("lokileech"),
		kong.Description("Fetch daily dnsmasq and neighbour logs from Loki into JSONL files."),
	)
	_, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)

	logLevel := slog.LevelInfo
	if commandLine.Verbose {
		logLevel = slog.LevelDebug
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	if err := lokileech.Run(context.Background(), lokileech.Config{
		Addr:             commandLine.Addr,
		Batch:            commandLine.Batch,
		Days:             commandLine.Days,
		DstPath:          commandLine.DstPath,
		ParallelDuration: commandLine.ParallelDuration,
		ParallelWorkers:  commandLine.ParallelWorkers,
		Query:            commandLine.Query,
		AlsoToday:        commandLine.AlsoToday,
	}); err != nil {
		slog.Error("lokileech failed", "error", err)
		os.Exit(1)
	}
}
