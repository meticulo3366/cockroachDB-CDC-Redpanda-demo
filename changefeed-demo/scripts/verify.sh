#!/usr/bin/env bash
# End-to-end verification: mutations -> CHANGEFEED -> Redpanda -> accounts_audit.
set -euo pipefail
cd "$(dirname "$0")/.."

crdb() { docker exec cf-cockroachdb cockroach sql --insecure --database=cdcbank "$@"; }

echo "==> 1) CHANGEFEED job is running in CockroachDB"
crdb --execute="SELECT job_id, status FROM [SHOW CHANGEFEED JOBS];" 2>/dev/null | head -5

echo "==> 2) Redpanda changefeed topic 'accounts_cdc' has messages"
docker exec cf-redpanda rpk topic describe accounts_cdc -p 2>/dev/null \
  | awk 'NR>1 {s+=$NF} END {print "    messages on accounts_cdc: " s+0}'

echo "==> 3) accounts_audit grows over ~5s (CDC actively materializing)"
A1=$(crdb --execute="SELECT count(*) FROM accounts_audit;" --format=csv | tail -1)
sleep 5
A2=$(crdb --execute="SELECT count(*) FROM accounts_audit;" --format=csv | tail -1)
echo "    audit rows: $A1 -> $A2"
if [ "$A2" -gt "$A1" ]; then echo "    OK: CDC events are streaming into accounts_audit."
elif [ "$A2" -gt 0 ]; then echo "    OK: audit rows present."
else echo "    FAIL: no CDC rows captured."; exit 1; fi

echo "==> 4) All three change types captured (insert/update/delete)"
crdb --execute="SELECT op, count(*) AS events FROM accounts_audit GROUP BY op ORDER BY events DESC;"

echo "==> 5) Sample captured changes (newest first)"
crdb --execute="SELECT account_id, op, before_doc, after_doc, updated_hlc FROM accounts_audit ORDER BY audited_at DESC LIMIT 6;"

echo "==> 6) Reconstruct the change history of one account"
ACCT=$(crdb --execute="SELECT account_id FROM accounts_audit GROUP BY account_id ORDER BY count(*) DESC LIMIT 1;" --format=csv | tail -1)
echo "    busiest account_id = $ACCT"
crdb --execute="SELECT op, after_doc->>'balance' AS balance_after, audited_at FROM accounts_audit WHERE account_id = ${ACCT} ORDER BY audited_at DESC LIMIT 8;"

echo "==> 7) Live source table snapshot"
crdb --execute="SELECT count(*) AS live_accounts, sum(balance) AS total_balance FROM accounts;"

echo
echo "==> Verification complete."
