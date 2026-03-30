# go-nfdump2parquet

`go-nfdump2parquet` converts a time-partitioned tree of `nfdump` files into a flat directory of parquet files without calling the external `nfdump` binary.

The current parser targets nfdump v2 files produced by nfdump 1.7.x.

The input tree must look like `YYYY/MM/DD/HH/nfcapd.*`. The output directory contains files named by the coverage they represent:

- previous months: `nfcap_YYYYMM.parquet`
- previous days in the current month: `nfcap_YYYYMMDD.parquet`
- hours in the current day: `nfcap_YYYYMMDDHH.parquet`

## Build, lint, and test

```bash
make lint
make test
make build
prek run --all-files
```

## Install

```bash
make install
```

## Usage

```bash
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/out
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/out --now 2026-03-30T12:00:00Z
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/out --parallelism 4
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/out -v
```

Flags:

- `--src`: root of the `YYYY/MM/DD/HH/nfcapd.*` hierarchy
- `--dst`: flat output directory for parquet files
- `--now`: optional RFC3339 timestamp used to decide month/day/hour rollups
- `--parallelism` / `-j`: parser workers per parquet output; `0` auto-tunes
- `-v`: enable debug logging

## Example

If `--now` is `2026-03-30T12:00:00Z`, these inputs:

- `2026/01/08/03/nfcapd.202601080320`
- `2026/01/08/04/nfcapd.202601080420`
- `2026/03/29/10/nfcapd.202603291000`
- `2026/03/30/03/nfcapd.202603300320`

produce:

- `nfcap_202601.parquet`
- `nfcap_20260329.parquet`
- `nfcap_2026033003.parquet`

## Refresh behavior

Each parquet file carries embedded metadata describing the exact source files, sizes, and mtimes used to build it. The tool rebuilds a parquet file when:

- the parquet file does not exist
- any source file in that covered period changed
- any source file in that covered period was added
- some, but not all, source files in that covered period were removed
- a finer-grained file needs to be rolled up into a daily or monthly replacement

Cleanup rules:

- hourly parquet files are deleted after a daily replacement for that day exists
- daily parquet files are deleted after a monthly replacement for that month exists
- if a parquet file has no source files left at all, it is preserved

The tool ignores files and directories that do not match the expected `YYYY/MM/DD/HH/nfcapd.*` layout. Future-dated matching inputs still fail explicitly.

## Output schema

Each parquet row contains:

- `time_start_ns`
- `time_end_ns`
- `duration_ns`
- `protocol`
- `src_ip`
- `dst_ip`
- `src_port`
- `dst_port`
- `packets`
- `bytes`

When present in the source record, these optional columns are also emitted:

- `router_ip`
- `next_hop_ip`
- `src_as`
- `dst_as`
- `src_mask`
- `dst_mask`
- `tcp_flags`
