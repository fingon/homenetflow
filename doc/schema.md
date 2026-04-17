# homenetflow Data Schema

`homenetflow` persists several data formats across the ingest, enrichment, and UI pipeline. This document is the reference for those on-disk formats.

## Storage Overview

The pipeline reads and writes these artifacts:

- raw flow captures in a `YYYY/MM/DD/HH/nfcapd.*` tree
- base flow parquet files named `nfcap_<period>.parquet`
- daily Loki log exports named `YYYY-MM-DD.jsonl`
- enriched flow parquet files named `nfcap_<period>.parquet`
- DNS lookup parquet files named `dns_lookups_<period>.parquet`
- reverse DNS cache entries in `reverse_dns_cache.jsonl`
- UI summary parquet files named `ui_summary_*.parquet`

`<period>` is one of:

- `YYYYMM` for monthly files
- `YYYYMMDD` for daily files
- `YYYYMMDDHH` for hourly files

## File Layouts

### Raw nfdump input

`nfdump2parquet` reads a source tree laid out as:

```text
YYYY/MM/DD/HH/nfcapd.*
```

Files outside that layout are ignored.

### Base and enriched flow parquet

Base parquet written by `nfdump2parquet` and enriched parquet written by `parquethosts` both use flat filenames:

- `nfcap_YYYYMM.parquet`
- `nfcap_YYYYMMDD.parquet`
- `nfcap_YYYYMMDDHH.parquet`

### Daily log exports

`lokileech` writes one daily JSONL file per day:

- `YYYY-MM-DD.jsonl`

These files may contain dnsmasq entries, neighbour-table entries, or both.

### DNS lookup parquet

`parquethosts` also writes DNS lookup parquet beside enriched flow parquet:

- `dns_lookups_YYYYMM.parquet`
- `dns_lookups_YYYYMMDD.parquet`
- `dns_lookups_YYYYMMDDHH.parquet`

### Reverse DNS cache

Successful PTR lookups are appended to:

- `reverse_dns_cache.jsonl`

This file lives in the `parquethosts` destination directory.

### UI summary parquet

`parquetflowui` builds summary parquet files in the enriched parquet directory with names derived from the source period:

- `ui_summary_edges_tld_<period>.parquet`
- `ui_summary_edges_2ld_<period>.parquet`
- `ui_summary_bucketed_edges_tld_<period>.parquet`
- `ui_summary_bucketed_edges_2ld_<period>.parquet`
- `ui_summary_histogram_<period>.parquet`

When matching DNS lookup parquet exists, the UI also writes:

- `ui_summary_dns_edges_tld_<period>.parquet`
- `ui_summary_dns_edges_2ld_<period>.parquet`
- `ui_summary_dns_bucketed_edges_tld_<period>.parquet`
- `ui_summary_dns_bucketed_edges_2ld_<period>.parquet`
- `ui_summary_dns_histogram_<period>.parquet`

## Base Flow Parquet Schema

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

Optional columns are emitted when present in the source record:

- `direction`
- `router_ip`
- `next_hop_ip`
- `src_as`
- `dst_as`
- `src_mask`
- `dst_mask`
- `tcp_flags`

Field notes:

- `time_start_ns`, `time_end_ns`, and `duration_ns` are nanoseconds
- `ip_version` is `4` for IPv4, `6` for IPv6, and `0` when the source record does not expose an IP version
- `protocol` is the numeric IP protocol value
- `direction` is optional because source records may not include it

Each base parquet file embeds a refresh manifest in parquet metadata under key `go-nfdump2parquet.manifest`.

## Enriched Flow Parquet Schema

Enriched parquet preserves all base flow columns and adds:

- `src_host`
- `dst_host`
- `src_2ld`
- `dst_2ld`
- `src_tld`
- `dst_tld`
- `src_is_private`
- `dst_is_private`

Field meaning:

- `_host`: normalized hostname chosen for the IP
- `_2ld`: one label above the suffix, such as `iki.fi` from `www.fingon.iki.fi`
- `_tld`: suffix value, such as `fi` from `www.fingon.iki.fi` or `co.uk` from `foo.bar.co.uk`
- `_is_private`: whether the IP falls into the private or local ranges recognized by enrichment

For local names, derivation falls back to labels. For example, `cer.lan` produces `_2ld=cer.lan` and `_tld=lan`.

Each enriched parquet file embeds an enrichment manifest in parquet metadata under key `homenetflow.parquethosts.manifest`.

## JSONL Log Schemas

### Loki export wrapper

Each line in a daily Loki export is a JSON object with this outer shape:

```json
{
  "line": "...",
  "timestamp": "2026-04-10T12:00:00Z"
}
```

`line` contains the original log payload encoded as a JSON string.

### dnsmasq nested JSON entries

Structured dnsmasq entries use a nested JSON object in `line` with fields such as:

```json
{
  "answers": ["192.0.2.10"],
  "client_ip": "192.0.2.2",
  "message": "",
  "query_name": "example.org",
  "query_type": "A",
  "timestamp_end": "2026-04-10T12:00:00Z"
}
```

Relevant behavior:

- `query_name` and `answers` are used to associate names with returned IPs
- `client_ip`, `query_name`, `query_type`, and normalized `answers` feed DNS lookup parquet
- `timestamp_end` is preferred for lookup timing when present

The parser also accepts legacy dnsmasq `message` entries where the nested object contains `message` text such as `reply ... is ...`, `cached ... is ...`, `config ... is ...`, or hosts-file lines ending in `... is <ip>`.

### neighbour log entries

Neighbour-table entries use a nested JSON object in `line` with:

```json
{
  "dst": "192.0.2.10",
  "lladdr": "aa:bb:cc:dd:ee:ff"
}
```

`dst` is the observed IP address and `lladdr` is the link-layer address. These entries are used to map some IPv6 flows back to a matching IPv4 identity before hostname resolution.

## Reverse DNS Cache Schema

Each line in `reverse_dns_cache.jsonl` is:

```json
{
  "host": "device.example",
  "ip": "192.0.2.10"
}
```

Only successful PTR lookups are persisted. Misses are cached only in memory for the current run.

## DNS Lookup Parquet Schema

Each DNS lookup parquet row contains:

- `answer`
- `client_2ld`
- `client_host`
- `client_ip`
- `client_ip_version`
- `client_is_private`
- `client_tld`
- `lookups`
- `query_2ld`
- `query_name`
- `query_tld`
- `query_type`
- `time_start_ns`

Field notes:

- `answer` is the normalized DNS answer value, including `NXDOMAIN` when applicable
- `lookups` is the aggregated lookup count for the row
- `client_*` fields describe the querying client
- `query_*` fields describe the queried name

Each DNS lookup parquet file embeds the same enrichment manifest shape as enriched flow parquet under metadata key `homenetflow.dnslookups.manifest`.

## Embedded Manifest Schemas

### `SourceManifest`

Used inside the other manifests:

```json
{
  "path": "2026-04-10.jsonl",
  "sizeBytes": 1234,
  "mtimeNs": 1744286400000000000
}
```

### `RefreshManifest`

Stored in base parquet metadata key `go-nfdump2parquet.manifest`:

```json
{
  "version": 2,
  "sources": [
    {
      "path": "2026/04/10/12/nfcapd.202604101200",
      "sizeBytes": 1234,
      "mtimeNs": 1744286400000000000
    }
  ]
}
```

### `EnrichmentManifest`

Stored in enriched parquet metadata key `homenetflow.parquethosts.manifest` and DNS lookup parquet metadata key `homenetflow.dnslookups.manifest`:

```json
{
  "logicVersion": 5,
  "logs": [
    {
      "path": "2026-04-10.jsonl",
      "sizeBytes": 1234,
      "mtimeNs": 1744286400000000000
    }
  ],
  "skipDnsLookups": false,
  "source": {
    "path": "nfcap_2026041012.parquet",
    "sizeBytes": 1234,
    "mtimeNs": 1744286400000000000
  },
  "version": 1
}
```

### `UISummaryManifest`

Stored in UI summary parquet metadata key `homenetflow.parquetflowui.summary.manifest`:

```json
{
  "granularity": "tld",
  "kind": "edges",
  "logicVersion": 8,
  "source": {
    "path": "nfcap_202604.parquet",
    "sizeBytes": 1234,
    "mtimeNs": 1744286400000000000
  },
  "spanEndNs": 1744329600000000000,
  "spanStartNs": 1741737600000000000,
  "version": 1
}
```

Manifest metadata is used to decide whether outputs are fresh or need rebuilding.

## UI Summary Parquet Schemas

### Edge summaries

`ui_summary_edges_*` and `ui_summary_dns_edges_*` rows contain:

- `src_entity`
- `dst_entity`
- `bytes`
- `connections`
- `direction`
- `dst_private_bytes`
- `dst_private_connections`
- `dst_public_bytes`
- `dst_public_connections`
- `first_seen_ns`
- `ip_version`
- `last_seen_ns`
- `nxdomain_lookups`
- `src_private_bytes`
- `src_private_connections`
- `src_public_bytes`
- `src_public_connections`
- `successful_lookups`

### Bucketed edge summaries

`ui_summary_bucketed_edges_*` and `ui_summary_dns_bucketed_edges_*` rows contain the same fields as edge summaries plus:

- `bucket_start_ns`

### Histogram summaries

`ui_summary_histogram_*` and `ui_summary_dns_histogram_*` rows contain:

- `bucket_start_ns`
- `bytes`
- `connections`
- `direction`
- `ip_version`
