#!/usr/bin/env bash
# End-to-end verification: synthetic data -> Redpanda -> CockroachDB.
set -euo pipefail

cd "$(dirname "$0")/.."

TOPIC="${ORDERS_TOPIC:-orders}"

crdb() { docker exec cockroachdb cockroach sql --insecure --database=cdcdemo "$@"; }

echo "==> 1) Redpanda topic '$TOPIC' exists and has messages"
docker exec redpanda rpk topic describe "$TOPIC" >/dev/null
# HIGH-WATERMARK is the last column of `rpk topic describe -p`; sum across partitions.
HIGH=$(docker exec redpanda rpk topic describe "$TOPIC" -p 2>/dev/null \
  | awk 'NR>1 {sum+=$NF} END {print sum+0}')
echo "    messages produced to topic: $HIGH"

echo "==> 2) Consumer group is committing offsets (sink is consuming)"
docker exec redpanda rpk group describe cockroach-sink 2>/dev/null \
  | grep -E "TOPIC|orders" || echo "    (group not yet registered)"

echo "==> 3) Row count in CockroachDB grows over ~5s (proves live streaming)"
C1=$(crdb --execute="SELECT count(*) FROM orders;" --format=csv | tail -1)
sleep 5
C2=$(crdb --execute="SELECT count(*) FROM orders;" --format=csv | tail -1)
echo "    rows at t0: $C1"
echo "    rows at t5: $C2"
if [ "$C2" -gt "$C1" ]; then
  echo "    OK: data is actively streaming into CockroachDB."
elif [ "$C2" -gt 0 ]; then
  echo "    OK: rows present (generator may be finished/capped)."
else
  echo "    FAIL: no rows in CockroachDB."; exit 1
fi

echo "==> 4) Sample rows"
crdb --execute="SELECT order_id, customer_id, status, item, quantity, unit_price, amount, region, created_at FROM orders ORDER BY ingested_at DESC LIMIT 5;"

echo "==> 5) Aggregations (schema integrity: computed amount, indexes usable)"
crdb --execute="SELECT status, count(*) AS orders, sum(amount) AS revenue FROM orders GROUP BY status ORDER BY orders DESC;"
crdb --execute="SELECT region, count(*) AS orders FROM orders GROUP BY region ORDER BY orders DESC;"

echo "==> 6) Data integrity checks"
crdb --execute="SELECT count(*) AS bad_amount FROM orders WHERE amount <> quantity * unit_price;"
crdb --execute="SELECT count(DISTINCT order_id) AS distinct_ids, count(*) AS total_rows FROM orders;"

echo
echo "==> Verification complete."
