package main

import (
	"log/slog"
	"os"

	"github.com/fingon/homenetflow/internal/parquetui"
)

func main() {
	if err := parquetui.Run(os.Args[1:]); err != nil {
		slog.Error("parquetflowui failed", "err", err)
		os.Exit(1)
	}
}
