#!/usr/bin/env bash
# Connector (Redpanda Connect pipeline) health & throughput.
#
# Self-hosted Redpanda Connect has no Console UI page (that's Cloud-only), but
# each pipeline exposes an HTTP status API. This reads /ready and /stats from
# both pipelines and prints a compact health table.
#
#   ./scripts/connectors.sh            # one snapshot
#   ./scripts/connectors.sh --watch    # refresh every 3s until Ctrl-C
set -euo pipefail

# name:host_port pairs (host ports mapped in docker-compose.yml)
CONNECTORS=( "generator:4195" "sink:4196" )

metric() { # $1=stats blob  $2=metric name -> value (0 if absent)
  printf '%s\n' "$1" | awk -v m="$2" '$0 ~ "^"m"\\{" {print $2; found=1} END{if(!found) print 0}'
}

snapshot() {
  printf '%s\n' "──────────────────────────────────────────────────────────────────────────"
  printf ' %-10s %-7s %-7s %-8s %12s %12s %8s\n' \
    "CONNECTOR" "PING" "READY" "CONNS" "IN(recv)" "OUT(sent)" "ERRORS"
  printf '%s\n' "──────────────────────────────────────────────────────────────────────────"
  for c in "${CONNECTORS[@]}"; do
    name="${c%%:*}"; port="${c##*:}"; base="http://localhost:${port}"

    ping=$(curl -s -m 3 "$base/ping" 2>/dev/null || echo "-")
    [ "$ping" = "pong" ] && ping="up" || { ping="DOWN"; }

    ready_json=$(curl -s -m 3 "$base/ready" 2>/dev/null || echo "")
    if printf '%s' "$ready_json" | grep -q '"connected":false' || [ -z "$ready_json" ]; then
      ready="NOT-READY"
    else
      ready="ready"
    fi

    stats=$(curl -s -m 3 "$base/stats" 2>/dev/null || echo "")
    in_recv=$(metric "$stats" input_received)
    out_sent=$(metric "$stats" output_sent)
    out_err=$(metric "$stats" output_error)
    in_up=$(metric "$stats" input_connection_up)
    out_up=$(metric "$stats" output_connection_up)
    conns="${in_up}in/${out_up}out"

    printf ' %-10s %-7s %-7s %-8s %12s %12s %8s\n' \
      "$name" "$ping" "$ready" "$conns" "$in_recv" "$out_sent" "$out_err"
  done
  printf '%s\n' "──────────────────────────────────────────────────────────────────────────"
  printf ' generator: synthetic orders -> Redpanda   |   sink: Redpanda -> CockroachDB\n'
  printf ' READY=input&output connected, CONNS=connection_up flags, ERRORS=output_error\n'
}

if [ "${1:-}" = "--watch" ]; then
  trap 'echo; echo "stopped."; exit 0' INT
  while true; do clear; date '+%H:%M:%S'; snapshot; done
else
  snapshot
fi
