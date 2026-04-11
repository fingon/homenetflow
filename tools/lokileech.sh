#!/usr/bin/env bash
#
# To use this, you need: logcli installed (brew install logcli)
#
# It fetches daily logs to single files from Grafana if they haven't
# been fetched already. Customize your LOKI_ADDR and QUERY as needed.
#
set -euo pipefail

LOKI_ADDR="https://fw.fingon.iki.fi:3100"
QUERY='{source=~"dnsmasq|ip_neighbour"}'
DAYS=80   # bit less than 3 months - I have 90 day retention


for i in $(seq 1 "$DAYS"); do
  day=$(gdate -d "$i day ago" +"%Y-%m-%d")        # 1= yesterday
  out="${day}.jsonl"

  # Skip if file already exists and not empty
  if [ -s "$out" ]; then
    echo "Skipping $day (file exists and non-empty)"
    continue
  fi

  from="${day}T00:00:00Z"
  to="$(gdate -d "${day} +1 day" +"%Y-%m-%d")T00:00:00Z"

  echo "Fetching $day -> $out"
  # echo "Fetching $day -> $out ( $from - $to )"
  logcli \
    --addr="$LOKI_ADDR" \
    query \
    --parallel-duration=15m \
    --parallel-max-workers=10 \
    --output=jsonl \
    --timezone=UTC \
    --from="$from" \
    --to="$to" \
    --limit=0 \
    --batch=5000 \
    "$QUERY" > "$out".new 2> "$out".stderr \
      && mv "$out".new "$out" || rm -f "$out".new

# NB: If you get error about batch size being too small, incrase it -
# at least 1k doesn't seem to work for me.

# if using auth:
#    --username="$TENANT_ID" \
#    --password="$GRAFANA_TOKEN" \

  # Delete empty output if any, to prevent cache poisoning
  if [ ! -s "$out" ]; then
    rm -f "$out"
  fi
done
