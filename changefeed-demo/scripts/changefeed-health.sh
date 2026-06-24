#!/usr/bin/env bash
# CockroachDB-side health of the CHANGEFEED: status, forward progress, and
# emitted-message metrics. This is the authoritative "is CDC working?" check.
#   ./scripts/changefeed-health.sh
set -euo pipefail
cd "$(dirname "$0")/.."

crdb() { docker exec cf-cockroachdb cockroach sql --insecure --format=csv -e "$1" 2>/dev/null | tail -1; }

STATUS=$(crdb "SELECT status FROM [SHOW CHANGEFEED JOBS] LIMIT 1;")
RUNNING_STATUS=$(crdb "SELECT running_status FROM [SHOW CHANGEFEED JOBS] LIMIT 1;")
RUNNING=$(crdb "SELECT value::INT FROM crdb_internal.node_metrics WHERE name='changefeed.running';")

HW1=$(crdb "SELECT (high_water_timestamp::decimal/1e9)::INT FROM [SHOW CHANGEFEED JOBS] LIMIT 1;")
MSG1=$(crdb "SELECT value::INT FROM crdb_internal.node_metrics WHERE name='changefeed.emitted_messages';")
sleep 6
HW2=$(crdb "SELECT (high_water_timestamp::decimal/1e9)::INT FROM [SHOW CHANGEFEED JOBS] LIMIT 1;")
MSG2=$(crdb "SELECT value::INT FROM crdb_internal.node_metrics WHERE name='changefeed.emitted_messages';")
BYTES=$(crdb "SELECT value::INT FROM crdb_internal.node_metrics WHERE name='changefeed.emitted_bytes';")

hw_label()  { if [ "${1:-0}" -gt "${2:-0}" ]; then printf '▲ advancing (+%ss)  OK' "$(( $1 - $2 ))"; else printf '— frozen  CHECK SINK'; fi; }
msg_label() { if [ "${1:-0}" -gt "${2:-0}" ]; then printf '▲ +%d  emitting' "$(( $1 - $2 ))"; else printf '— flat'; fi; }
st_label()  { [ "$1" = "running" ] && printf 'OK' || printf 'NOT RUNNING'; }

printf '%s\n' "──────────────────────────────────────────────────────────────"
printf ' CockroachDB CHANGEFEED health\n'
printf '%s\n' "──────────────────────────────────────────────────────────────"
printf ' %-22s %-12s %s\n' "job status"           "$STATUS"   "$(st_label "$STATUS")"
printf ' %-22s %-12s %s\n' "active changefeeds"    "$RUNNING"  "(changefeed.running)"
printf ' %-22s %-12s %s\n' "high-water progress"   "${HW2}s"   "$(hw_label "$HW2" "$HW1")"
printf ' %-22s %-12s %s\n' "emitted messages"      "$MSG2"     "$(msg_label "$MSG2" "$MSG1")"
printf ' %-22s %-12s\n'    "emitted bytes"         "$BYTES"
printf '%s\n' "──────────────────────────────────────────────────────────────"
printf ' running_status: %s\n' "$RUNNING_STATUS"
printf ' status=running + high-water advancing + emitted climbing = CDC healthy\n'
