# homenetflow Features

`homenetflow` turns raw network-flow captures into something you can browse and investigate. The typical flow is:

1. `nfdump2parquet` converts raw `nfdump` captures into flat parquet files.
2. `lokileech` fetches DNS and neighbour logs that help identify hosts.
3. `parquethosts` enriches parquet files with host and domain information.
4. `parquetflowui` serves a browser UI for exploring the enriched traffic.

## `nfdump2parquet`

`nfdump2parquet` reads a `YYYY/MM/DD/HH/nfcapd.*` capture tree and writes flat parquet outputs that are easier to reuse in later steps.

User-visible behavior:

- converts raw `nfdump` v2 captures without depending on the external `nfdump` binary
- writes hourly, daily, or monthly parquet files depending on how old the source data is
- rebuilds outputs when covered source files are added or changed
- removes superseded hourly and daily parquet files after rolling them up into larger files
- keeps already-built parquet files when their whole source period disappears, instead of deleting them
- ignores files outside the expected directory layout
- fails clearly on future-dated matching inputs

## `lokileech`

`lokileech` pulls daily Loki logs into local `YYYY-MM-DD.jsonl` files for later enrichment.

User-visible behavior:

- fetches dnsmasq logs and neighbour-table logs without using the external `logcli` binary
- writes one JSONL file per day
- skips existing non-empty outputs so reruns stay fast
- can refresh the newest day and include today with `--also-today`
- writes through a temporary `.new` file and only replaces the final file when the fetch succeeds
- lets users control the fetch window, query size, parallelism, and Loki address

## `parquethosts`

`parquethosts` reads flat parquet files plus the daily logs and writes a second parquet set with host-oriented fields added.

User-visible behavior:

- adds resolved hostnames for source and destination IPs
- derives `_2ld` and `_tld` values from resolved names
- marks source and destination addresses as private/local or public
- uses dnsmasq observations first, then cached reverse DNS, then seeds missing cache entries from structured dnsmasq `PTR` results, then structured dnsmasq `A`/`AAAA` answers, then falls back to live PTR lookups
- can use neighbour-table data to mark observed IPv6 `/64`s local and map some IPv6 traffic back to a matching IPv4 dnsmasq identity before resolving names
- keeps a persistent `reverse_dns_cache.jsonl` so later runs reuse successful public and RFC1918 IPv4 PTR results, persist PTR misses including structured-log `NXDOMAIN` results, ignore structured-log `SERVFAIL`, and prune local IPv6 entries
- can skip live DNS lookups with `--skip-dns-lookups`
- shows a progress bar while rebuilding enriched parquet files
- rebuilds outputs when source parquet, overlapping logs, or enrichment logic change
- deletes enriched parquet files whose source parquet file no longer exists

The enriched parquet output keeps the original flow columns and adds:

- `src_host`, `dst_host`
- `src_2ld`, `dst_2ld`
- `src_tld`, `dst_tld`
- `src_is_private`, `dst_is_private`

## `parquetflowui`

`parquetflowui` serves a browser UI for exploring enriched netflow data.

User-visible behavior:

- opens a server-rendered dashboard in the browser
- shows a timeline histogram for the current time range
- supports brush-based zooming on the timeline
- shows a graph of communicating entities and their connections
- lets users zoom and pan the graph
- scales graph nodes and edges by the selected metric
- switches between `bytes`, `connections`, and DNS lookup activity
- switches graph/table grouping between `tld`, `2ld`, `hostname`, and `ip`
- filters by address family (`all`, `ipv4`, `ipv6`)
- filters by direction (`both`, `ingress`, `egress`)
- filters by protocol and service port from traffic breakdown slices
- supports include and exclude filters for selected entities
- supports free-text search
- shows rankings and summary totals for the active view
- shows summary-backed traffic breakdown pies for protocols, IP family, and popular ports
- shows a sortable flows table with paging
- opens flow-detail views for a selected entity or edge when the time range is 7 days or less
- serves views over 7 days from UI summaries instead of raw flow rows
- supports ignored-traffic rules based on entities, host/IP identity, CIDR, protocol, service port, direction, and address family
- groups overflowed graph nodes into `Rest`
- colors nodes by address class so private, mixed, and public traffic are distinguishable
- uses local buckets for private/local `tld` and `2ld` entities and `Unknown public` for unresolved public entities
- refreshes automatically when underlying parquet files change
- rebuilds its summary parquet files automatically when they are stale

## End Result

Together, the tools give users a repeatable way to:

- convert raw flow archives into queryable parquet data
- attach host and domain context to those flows
- browse traffic patterns in a UI instead of raw logs
- move from broad summaries into individual flow details when investigating activity
