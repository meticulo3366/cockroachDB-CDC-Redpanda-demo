#!/bin/sh
# One-shot bootstrap for the CHANGEFEED demo, run by the crdb-init container.
#   1. Apply the schema (accounts + accounts_audit).
#   2. Enable rangefeeds (required for changefeeds).
#   3. Create the CHANGEFEED on accounts -> Redpanda (idempotent).
set -e

HOST="cockroachdb:26257"
SQL="cockroach sql --insecure --host=${HOST}"

echo "==> applying schema"
$SQL --file=/schema.sql

echo "==> enabling rangefeeds (required for changefeeds)"
$SQL -e "SET CLUSTER SETTING kv.rangefeed.enabled = true;"

echo "==> ensuring CHANGEFEED on cdcbank.public.accounts -> Redpanda"
RUNNING=$($SQL --format=csv -e \
  "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status='running';" | tail -1)

if [ "$RUNNING" = "0" ]; then
  $SQL -e "CREATE CHANGEFEED FOR TABLE cdcbank.public.accounts \
    INTO 'kafka://redpanda:9092?topic_name=accounts_cdc' \
    WITH updated, diff, resolved='15s', min_checkpoint_frequency='5s';"
  echo "==> changefeed created"
else
  echo "==> changefeed already running ($RUNNING); skipping"
fi

echo "changefeed-demo init complete"
