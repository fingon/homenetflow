# homenetflow

`homenetflow` contains two Go tools for turning flow captures into parquet and then enriching those parquet files with host-derived metadata.

- `nfdump2parquet`: converts a `YYYY/MM/DD/HH/nfcapd.*` tree into flat `nfcap_*.parquet` files
- `parquethosts`: reads those flat parquet files plus dnsmasq `.jsonl` logs and writes enriched parquet files with host, `_2ld`, and `_tld` fields

The current parser targets nfdump v2 files produced by nfdump 1.7.x.

## Build, Lint, and Test

```bash
rtk make lint
rtk make test
rtk make build
prek run --all-files
```

## Install

```bash
go install ./cmd/...
```

## `nfdump2parquet`

`nfdump2parquet` converts a time-partitioned tree of `nfdump` files into a flat directory of parquet files without calling the external `nfdump` binary.

### Input and Output Layout

The input tree must look like `YYYY/MM/DD/HH/nfcapd.*`.

The output directory contains files named by the coverage they represent:

- previous months: `nfcap_YYYYMM.parquet`
- previous days in the current month: `nfcap_YYYYMMDD.parquet`
- hours in the current day: `nfcap_YYYYMMDDHH.parquet`

### Usage

```bash
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/parquet
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/parquet --now 2026-03-30T12:00:00Z
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/parquet --parallelism 4
go run ./cmd/nfdump2parquet --src /flows/in --dst /flows/parquet -v
```

Flags:

- `--src`: root of the `YYYY/MM/DD/HH/nfcapd.*` hierarchy
- `--dst`: flat output directory for parquet files
- `--now`: optional RFC3339 timestamp used to decide month/day/hour rollups
- `--parallelism` / `-j`: parser workers per parquet output; `0` auto-tunes
- `-v`: enable debug logging

### Refresh Behavior

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

## `parquethosts`

`parquethosts` reads flat `nfcap_*.parquet` files plus dnsmasq daily logs and writes a second flat parquet directory with additional host-derived fields.

When enriched parquet files need rebuilding, the tool shows a stderr progress bar based on processed parquet rows.

### Inputs

- `--src-parquet`: flat directory containing `nfcap_YYYYMM.parquet`, `nfcap_YYYYMMDD.parquet`, and `nfcap_YYYYMMDDHH.parquet`
- `--src-log`: directory containing dnsmasq daily log files named `YYYY-MM-DD.jsonl`
- `--dst`: flat output directory for enriched parquet files

The dnsmasq logs may contain either:

- structured nested JSON entries with `answers`, `query_name`, and `timestamp_end`
- legacy `message` entries such as `reply ... is ...`, `cached ... is ...`, `config ... is ...`, or hosts-file lines ending in `... is <ip>`

### Usage

```bash
go run ./cmd/parquethosts --src-parquet /flows/parquet --src-log /flows/logs --dst /flows/parquet-hosts
go run ./cmd/parquethosts --src-parquet /flows/parquet --src-log /flows/logs --dst /flows/parquet-hosts -v
```

Flags:

- `--src-parquet`: flat input parquet directory
- `--src-log`: dnsmasq log directory
- `--dst`: flat output directory for enriched parquet files
- `-v`: enable debug logging

### Host Resolution Order

For each `src_ip` and `dst_ip`, `parquethosts` resolves names in this order:

1. newest matching dnsmasq observation for the IP where the log timestamp is older than or equal to `time_start_ns`
2. the dnsmasq observation must also be within one hour before the flow start
3. if no log match is found, a persistent reverse-DNS cache hit
4. if no cache hit is found, a live PTR lookup

Successful PTR results are appended to `<dst>/reverse_dns_cache.jsonl` and reused forever. PTR misses are cached only in memory for the current run. Malformed PTR responses are logged as warnings, treated as misses, and do not stop enrichment.

### Refresh Behavior

Each enriched parquet file carries embedded metadata describing:

- the exact source parquet file used to build it
- the overlapping dnsmasq log files used to enrich it

The tool rebuilds an enriched parquet file when:

- the destination parquet file does not exist
- the source parquet file changed
- a new overlapping log file appears
- an overlapping log file changes

The tool does not rebuild a parquet file only because an overlapping log file disappeared.

The tool also deletes destination `nfcap_*.parquet` files that no longer have a corresponding source parquet file.

### Enriched Columns

The enriched parquet output preserves all original columns and adds these optional fields:

- `src_host`
- `dst_host`
- `src_2ld`
- `dst_2ld`
- `src_tld`
- `dst_tld`

Field meaning:

- `_host`: normalized hostname chosen for the IP
- `_2ld`: one label above the `_tld`, such as `iki.fi` from `www.fingon.iki.fi`
- `_tld`: top-level suffix value, such as `fi` from `www.fingon.iki.fi` or `co.uk` from `foo.bar.co.uk`

For local names, the tool falls back to label-based derivation. For example, `cer.lan` produces `_2ld=cer.lan` and `_tld=lan`.

## End-to-End Example

Generate flat parquet from raw nfdump files:

```bash
go run ./cmd/nfdump2parquet \
  --src /data/nfdump \
  --dst /data/parquet \
  --now 2026-04-02T12:00:00Z
```

Then enrich that parquet with dnsmasq logs into a second output directory:

```bash
go run ./cmd/parquethosts \
  --src-parquet /data/parquet \
  --src-log /data/logs \
  --dst /data/parquet-hosts
```

If `nfdump2parquet` produces:

- `/data/parquet/nfcap_202603.parquet`
- `/data/parquet/nfcap_20260401.parquet`
- `/data/parquet/nfcap_2026040211.parquet`

then `parquethosts` writes matching enriched outputs:

- `/data/parquet-hosts/nfcap_202603.parquet`
- `/data/parquet-hosts/nfcap_20260401.parquet`
- `/data/parquet-hosts/nfcap_2026040211.parquet`

plus the persistent cache file:

- `/data/parquet-hosts/reverse_dns_cache.jsonl`

## Base Output Schema

Each base parquet row contains:

- `time_start_ns`
- `time_end_ns`
- `duration_ns`
- `ip_version`
- `protocol`
- `src_ip`
- `dst_ip`
- `src_port`
- `dst_port`
- `packets`
- `bytes`

`ip_version` is `4` for IPv4 flows, `6` for IPv6 flows, and `0` when the source record does not expose an IP version.

When present in the source record, these optional columns are also emitted:

- `router_ip`
- `next_hop_ip`
- `src_as`
- `dst_as`
- `src_mask`
- `dst_mask`
- `tcp_flags`
