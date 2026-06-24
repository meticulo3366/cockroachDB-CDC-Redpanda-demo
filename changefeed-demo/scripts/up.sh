#!/usr/bin/env bash
# Bring up the CHANGEFEED demo and wait until changes are flowing.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Starting stack (Redpanda, Console, CockroachDB, mutator, CDC consumer)..."
docker compose up -d

echo "==> Waiting for Redpanda and CockroachDB to become healthy..."
for svc in cf-redpanda cf-cockroachdb; do
  printf "    %-15s " "$svc"
  for _ in $(seq 1 40); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "starting")
    if [ "$status" = "healthy" ]; then echo "healthy"; break; fi
    sleep 3
  done
  [ "${status:-}" = "healthy" ] || { echo "NOT healthy"; exit 1; }
done

echo "==> Confirming schema + changefeed bootstrap (cf-crdb-init)..."
docker wait cf-crdb-init >/dev/null 2>&1 || true
docker logs cf-crdb-init 2>&1 | grep -q "init complete" \
  && echo "    init complete (changefeed running)" \
  || { echo "    init did not complete"; docker logs cf-crdb-init; exit 1; }

echo
echo "==> Stack is up. Endpoints:"
echo "    Redpanda Kafka API : localhost:29092"
echo "    Redpanda Console   : http://localhost:8089"
echo "    CockroachDB SQL    : postgres://root@localhost:26258/cdcbank?sslmode=disable"
echo "    CockroachDB Console: http://localhost:8081"
echo "    Mutator metrics    : http://localhost:4295/metrics"
echo "    CDC consumer metrics: http://localhost:4296/metrics"
echo
echo "Run ./scripts/verify.sh to confirm CDC events flow into accounts_audit."
