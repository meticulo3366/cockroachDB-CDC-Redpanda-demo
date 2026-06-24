#!/usr/bin/env bash
# Bring up the full demo stack and wait until data is flowing end-to-end.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Starting stack (Redpanda, Console, CockroachDB, Connect x2)..."
docker compose up -d

echo "==> Waiting for Redpanda and CockroachDB to become healthy..."
# Compose already gates dependents on health; this just surfaces status.
for svc in redpanda cockroachdb; do
  printf "    %-12s " "$svc"
  for _ in $(seq 1 40); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "starting")
    if [ "$status" = "healthy" ]; then echo "healthy"; break; fi
    sleep 3
  done
  [ "${status:-}" = "healthy" ] || { echo "NOT healthy"; exit 1; }
done

echo "==> Confirming schema bootstrap (crdb-init) completed..."
docker wait crdb-init >/dev/null 2>&1 || true
docker logs crdb-init 2>&1 | grep -q "schema applied" \
  && echo "    schema applied" \
  || { echo "    schema bootstrap did not report success"; docker logs crdb-init; exit 1; }

echo
echo "==> Stack is up. Endpoints:"
echo "    Redpanda Kafka API : localhost:19092"
echo "    Redpanda Console   : http://localhost:8088"
echo "    CockroachDB SQL    : postgres://root@localhost:26257/cdcdemo?sslmode=disable"
echo "    CockroachDB Console: http://localhost:8080"
echo "    Connect generator  : http://localhost:4195/metrics"
echo "    Connect sink       : http://localhost:4196/metrics"
echo
echo "Run ./scripts/verify.sh to confirm data is flowing into CockroachDB."
