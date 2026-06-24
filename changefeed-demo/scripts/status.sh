#!/usr/bin/env bash
# Live status of the CDC pipeline: mutations -> changefeed -> audit.
#   ./scripts/status.sh            # one snapshot (rates over 3s)
#   ./scripts/status.sh --watch    # refresh until Ctrl-C
set -euo pipefail
cd "$(dirname "$0")/.."
WINDOW=3

cdc_topic_total() {
  docker exec cf-redpanda rpk topic describe accounts_cdc -p 2>/dev/null \
    | awk 'NR>1 {s+=$NF} END {print s+0}'
}
audit_rows() {
  docker exec cf-cockroachdb cockroach sql --insecure --database=cdcbank \
    --format=csv --execute="SELECT count(*) FROM accounts_audit;" 2>/dev/null | tail -1
}
live_accounts() {
  docker exec cf-cockroachdb cockroach sql --insecure --database=cdcbank \
    --format=csv --execute="SELECT count(*) FROM accounts;" 2>/dev/null | tail -1
}

rate_label() { if [ "$1" -gt 0 ]; then printf '▲ +%d (~%d/s) ACTIVE' "$1" $(( $1 / WINDOW )); else printf '— idle'; fi; }

snapshot() {
  local c1 a1 c2 a2 live
  c1=$(cdc_topic_total); a1=$(audit_rows)
  sleep "$WINDOW"
  c2=$(cdc_topic_total); a2=$(audit_rows); live=$(live_accounts)
  printf '%s\n' "──────────────────────────────────────────────────────────────"
  printf ' CockroachDB CHANGEFEED → Redpanda → audit   (rates over %ss)\n' "$WINDOW"
  printf '%s\n' "──────────────────────────────────────────────────────────────"
  printf ' %-26s %10s   %s\n' "Changefeed → Redpanda" "$c2 msgs" "$(rate_label $((c2-c1)))"
  printf ' %-26s %10s   %s\n' "Consumer → accounts_audit" "$a2 rows" "$(rate_label $((a2-a1)))"
  printf ' %-26s %10s\n' "Live rows in accounts" "$live"
  printf '%s\n' "──────────────────────────────────────────────────────────────"
}

if [ "${1:-}" = "--watch" ]; then
  trap 'echo; echo stopped.; exit 0' INT
  while true; do clear; date '+%H:%M:%S'; snapshot; done
else
  snapshot
fi
