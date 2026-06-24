#!/usr/bin/env bash
# Live status of the pipeline: is Redpanda producing and is CockroachDB ingesting?
#
#   ./scripts/status.sh            # one snapshot (measures rate over 3s)
#   ./scripts/status.sh --watch    # refresh every 3s until Ctrl-C
set -euo pipefail

cd "$(dirname "$0")/.."

WINDOW=3   # seconds used to measure throughput

topic_total() {
  docker exec redpanda rpk topic describe orders -p 2>/dev/null \
    | awk 'NR>1 {s+=$NF} END {print s+0}'
}
group_lag() {
  docker exec redpanda rpk group describe cockroach-sink 2>/dev/null \
    | awk '/orders/ {print $6+0; found=1} END {if(!found) print "n/a"}'
}
crdb_rows() {
  docker exec cockroachdb cockroach sql --insecure --database=cdcdemo \
    --format=csv --execute="SELECT count(*) FROM orders;" 2>/dev/null | tail -1
}
crdb_last_ingest_age() {
  # Seconds since the most recently ingested row (0 = ingesting right now).
  docker exec cockroachdb cockroach sql --insecure --database=cdcdemo \
    --format=csv --execute="SELECT round(extract(epoch FROM now()-max(ingested_at)))::INT FROM orders;" \
    2>/dev/null | tail -1
}

snapshot() {
  local m1 r1 m2 r2 lag age dm dr
  m1=$(topic_total); r1=$(crdb_rows)
  sleep "$WINDOW"
  m2=$(topic_total); r2=$(crdb_rows)
  lag=$(group_lag); age=$(crdb_last_ingest_age)
  dm=$(( m2 - m1 )); dr=$(( r2 - r1 ))

  printf '%s\n' "──────────────────────────────────────────────────────────────"
  printf ' Redpanda → CockroachDB pipeline status   (rates over %ss)\n' "$WINDOW"
  printf '%s\n' "──────────────────────────────────────────────────────────────"
  printf ' %-22s %12s   %s\n' "Redpanda produced"  "$m2 msgs"  "$(rate_label "$dm")"
  printf ' %-22s %12s   %s\n' "CockroachDB ingested" "$r2 rows"  "$(rate_label "$dr")"
  printf ' %-22s %12s   %s\n' "Consumer lag (sink)" "$lag"      "$(lag_label "$lag")"
  printf ' %-22s %12s   %s\n' "Last row ingested"   "${age}s ago" "$(fresh_label "$age")"
  printf '%s\n' "──────────────────────────────────────────────────────────────"
}

rate_label() { # $1 = delta over WINDOW
  if [ "$1" -gt 0 ]; then printf '▲ +%d  (~%d/s)  ACTIVE' "$1" $(( $1 / WINDOW ));
  else printf '— no change  IDLE/STOPPED'; fi
}
lag_label() {
  case "$1" in
    n/a) printf 'group not registered yet' ;;
    *) if [ "$1" -le 100 ]; then printf 'OK — sink keeping up'; else printf 'behind — catching up'; fi ;;
  esac
}
fresh_label() {
  if [ "${1:-99}" -le 5 ] 2>/dev/null; then printf 'fresh — ingesting now'; else printf 'stale — check sink'; fi
}

if [ "${1:-}" = "--watch" ]; then
  trap 'echo; echo "stopped."; exit 0' INT
  while true; do clear; date '+%H:%M:%S'; snapshot; done
else
  snapshot
fi
