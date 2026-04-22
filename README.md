# homenetflow

`homenetflow` contains tools for turning flow captures into parquet, enriching those parquet files with host-derived metadata, and browsing the enriched parquet in a web UI.

- `nfdump2parquet`: converts a `YYYY/MM/DD/HH/nfcapd.*` tree into flat `nfcap_*.parquet` files
- `lokileech`: fetches daily dnsmasq and neighbour logs from Loki into `YYYY-MM-DD.jsonl` files
- `parquethosts`: reads those flat parquet files plus dnsmasq `.jsonl` logs and writes enriched parquet files with host, `_2ld`, and `_tld` fields
- `parquetflowui`: serves a web UI for browsing enriched parquet netflows with graph, timeline, and table views

For a user-facing overview of what each tool and the UI can do, see [doc/features.md](doc/features.md). For the persisted data formats, parquet columns, JSONL shapes, and embedded manifests, see [doc/schema.md](doc/schema.md).

The current parser targets nfdump v2 files produced by nfdump 1.7.x.

## Build, Lint, and Test

```bash
rtk make lint
rtk make test
rtk make build
rtk make ui
rtk make ui-watch
prek run --all-files
```

## Install

```bash
go install ./cmd/...
```

## `nfdump2parquet`

`nfdump2parquet` converts a time-partitioned tree of `nfdump` files into a flat directory of parquet files without calling the external `nfdump` binary.

Input/output filename layout is documented in [doc/schema.md](doc/schema.md).

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

## `lokileech`

`lokileech` fetches daily Loki logs into `YYYY-MM-DD.jsonl` files without calling the external `logcli` binary.

### Usage

```bash
go run ./cmd/lokileech --dst data/logs
go run ./cmd/lokileech --dst data/logs --also-today
go run ./cmd/lokileech --dst data/logs --addr https://fw.fingon.iki.fi:3100 -v
```

Flags:

- `--dst`: output directory for daily log files, default `.`
- `--addr`: Loki server address, default `LOKI_ADDR` or the home Loki endpoint
- `--query`: LogQL query, default `{source=~"dnsmasq|ip_neighbour"}`
- `--days`: number of daily files to fetch, default `80`
- `--batch`: Loki query batch size, default `5000`
- `--parallel-duration`: duration of each parallel query range, default `15m`
- `--parallel-workers`: maximum parallel workers per day, default `10`
- `--also-today`: delete the newest existing daily output and include today's logs
- `-v`: enable debug logging

The tool skips non-empty existing outputs. Missing or empty outputs are fetched through a `.new` file and atomically renamed when the daily fetch succeeds.

## `parquethosts`

`parquethosts` reads flat `nfcap_*.parquet` files plus daily dnsmasq and neighbour logs, then writes a second flat parquet directory with additional host-derived fields.

When enriched parquet files need rebuilding, the tool shows a stderr progress bar based on processed parquet rows.

### Inputs

- `--src-parquet`: flat directory containing `nfcap_YYYYMM.parquet`, `nfcap_YYYYMMDD.parquet`, and `nfcap_YYYYMMDDHH.parquet`
- `--src-log`: directory containing daily log files named `YYYY-MM-DD.jsonl`
- `--dst`: flat output directory for enriched parquet files

The log, cache, and parquet schemas used by enrichment are documented in [doc/schema.md](doc/schema.md).

### Usage

```bash
go run ./cmd/parquethosts --src-parquet /flows/parquet --src-log /flows/logs --dst /flows/parquet-hosts
go run ./cmd/parquethosts --src-parquet /flows/parquet --src-log /flows/logs --dst /flows/parquet-hosts -v
go run ./cmd/parquethosts --src-parquet /flows/parquet --src-log /flows/logs --dst /flows/parquet-hosts --skip-dns-lookups
```

Flags:

- `--src-parquet`: flat input parquet directory
- `--src-log`: daily log directory
- `--dst`: flat output directory for enriched parquet files
- `--skip-dns-lookups`: skip live PTR lookups and use only dnsmasq logs plus existing reverse DNS cache entries
- `-v`: enable debug logging

### Host Resolution Order

For each `src_ip` and `dst_ip`, `parquethosts` resolves names in this order:

1. for IPv6 addresses, try a same-file MAC-derived IPv4 mapping from non-zero `in_src_mac`, `in_dst_mac`, `out_src_mac`, and `out_dst_mac`
2. if MAC mapping does not produce an IPv4 candidate, try a non-conflicting neighbour-table `lladdr` mapping
3. for local IPv6 addresses in a neighbour-observed `/64`, use a mapped private IPv4 only when dnsmasq logs resolve it
4. newest matching dnsmasq observation for the selected IP where the log timestamp is older than or equal to `time_start_ns`
5. the dnsmasq observation must also be within one hour before the flow start
6. for public IPs and RFC1918 IPv4 only, if no log match is found, a persistent reverse-DNS cache hit
7. for public IPs and RFC1918 IPv4 only, if no cache hit is found, seed the reverse-DNS cache from the newest matching dnsmasq `A` or `AAAA` answer at or before `time_start_ns`
8. for public IPs and RFC1918 IPv4 only, if neither logs nor cache provide a cache entry, perform a live PTR lookup

Successful public and RFC1918 IPv4 PTR results are appended to `<dst>/reverse_dns_cache.jsonl` and reused forever. When a cache entry is missing, the cache is first seeded from structured dnsmasq `A` and `AAAA` answers already present in the loaded logs, using the newest eligible answer at or before the lookup time even if it is older than the one-hour direct-resolution window. PTR misses are also persisted with their lookup time so later runs skip re-querying those IPs, and a newer eligible dnsmasq `A` or `AAAA` answer for the same IP can promote a cached miss into a positive cache entry. Local IPv6 prefix entries are pruned from the cache before enrichment uses it. Malformed PTR responses are logged as warnings, treated as misses, and do not stop enrichment.
When `--skip-dns-lookups` is enabled, step 8 is skipped. Existing `reverse_dns_cache.jsonl` entries and dnsmasq log observations are still used, including log-based cache seeding.

The base and enriched flow parquet files now preserve the raw nfdump MAC fields as optional `in_src_mac`, `in_dst_mac`, `out_src_mac`, and `out_dst_mac` columns when the source record exposes non-zero values.

MAC-derived IPv6-to-IPv4 mappings are applied conservatively. For each source parquet file, `parquethosts` builds a same-file MAC-to-IPv4 index from IPv4 rows and prefers that over neighbour-table `lladdr` matching. It first uses the newest earlier IPv4 observation for the same MAC and otherwise falls back to a unique IPv4 only when that MAC maps to exactly one IPv4 in the file. Neighbour-table IPv6-to-IPv4 mappings remain as a lower-priority fallback. If the same IPv6 address is observed with multiple neighbour-log link-layer addresses, the neighbour mapping is ignored for that IPv6 address. Any IPv6 address in a neighbour-observed `/64` is treated as local; if it cannot be tied to a private IPv4 address with a dnsmasq name, the host, 2LD, and TLD are recorded as `Local IPv6`. Unnamed RFC1918 IPv4 addresses are recorded as `Local IPv4`. Named local addresses use the full hostname, the first hostname label as 2LD, and `Local` as TLD. Future neighbour-log changes do not keep invalidating already rebuilt older parquet outputs.

### Refresh Behavior

Each enriched parquet file carries embedded metadata describing:

- the exact source parquet file used to build it
- the overlapping dnsmasq log files used to enrich it
- the enrichment `logicVersion` used to produce the file

The tool rebuilds an enriched parquet file when:

- the destination parquet file does not exist
- the source parquet file changed
- a new overlapping log file appears
- an overlapping log file changes
- the stored enrichment `logicVersion` differs from the current enrichment logic

The tool does not rebuild a parquet file only because an overlapping log file disappeared.

The tool also deletes destination `nfcap_*.parquet` files that no longer have a corresponding source parquet file.

The enriched parquet columns and sidecar file formats are documented in [doc/schema.md](doc/schema.md).

## `parquetflowui`

`parquetflowui` serves a mostly server-rendered browser UI for investigating enriched parquet netflows.

The UI includes:

- a server-rendered graph of aggregated entities and connections
- a server-rendered timeline histogram with brush-based zooming
- metric scaling by bytes or connections
- global granularity switching across `tld`, `2ld`, `hostname`, and `ip`
- entity selection, include/exclude filtering, and a sortable flows table
- capped node counts at granular levels with a `Rest` bucket
- private-aware graph coloring: private nodes are green, mixed nodes are yellow, public nodes use the default blue

The UI uses htmx for navigation and filter updates, with only a small amount of custom JavaScript for histogram brushing and request status handling.

### Input

- flat directory containing enriched parquet output from `parquethosts`

The UI expects enriched parquet files and validates that they carry the enrichment manifest metadata. It also builds summary parquet files; their schema and metadata are documented in [doc/schema.md](doc/schema.md).

At `tld` and `2ld` granularities, local entities use `Local IPv4`, `Local IPv6`, or named local buckets. Public unresolved entities use `Unknown public`.

Views over 7 days are served from UI summaries instead of raw flow rows. Ignored-traffic rules use summary-safe dimensions: entities, host/IP identity, CIDR, protocol, service port, direction, and address family.

### Usage

```bash
go run ./cmd/parquetflowui /flows/parquet-hosts
go run ./cmd/parquetflowui /flows/parquet-hosts --port 8081
go run ./cmd/parquetflowui /flows/parquet-hosts --dev
go run ./cmd/parquetflowui /flows/parquet-hosts -v
go run ./cmd/parquetflowui /flows/parquet-hosts --pid-file /tmp/parquetflowui.pid --replace-running
rtk make ui
rtk make ui-watch
```

Flags:

- `--src-parquet`: flat input directory containing enriched `nfcap_*.parquet` files
- `--port`: HTTP port, default `8080`
- `--dev`: enable development-mode hot reload support
- `--pid-file`: write the running process ID to this file
- `--replace-running`: stop the process recorded in `--pid-file` before binding the new server
- `--reload-interval`: polling interval for parquet refresh, default `1m`
- `-v`: enable debug logging

The `rtk make ui` target runs:

```bash
go run ./cmd/parquetflowui --src-parquet data/parquet
```

Open `http://localhost:8080` after starting the server.

For development hot reloading, `rtk make ui-watch` runs the UI under `watchman-make`, starts a new `parquetflowui` on source changes, and lets the new process replace the old one using the pid file before reloading already-open browser tabs.

## End-to-End Example

Generate flat parquet from raw nfdump files:

```bash
go run ./cmd/nfdump2parquet \
  --src /data/nfdump \
  --dst /data/parquet \
  --now 2026-04-02T12:00:00Z
```

Then enrich that parquet with daily logs into a second output directory:

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

## Data Schema

For the base parquet schema, enriched parquet columns, JSONL log shapes, reverse DNS cache format, DNS lookup parquet, and UI summary parquet, see [doc/schema.md](doc/schema.md).
