# Turnkey targets for the CockroachDB CHANGEFEED -> Redpanda CDC demo.
.PHONY: help up down verify status watch changefeed-health logs lint psql jobs topic clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Start the stack and wait until healthy
	./scripts/up.sh

verify: ## Verify mutations -> changefeed -> Redpanda -> accounts_audit
	./scripts/verify.sh

status: ## One snapshot of CDC throughput
	./scripts/status.sh

watch: ## Live CDC status dashboard (Ctrl-C to stop)
	./scripts/status.sh --watch

down: ## Stop the stack and remove volumes
	./scripts/down.sh

clean: down ## Alias for down

logs: ## Tail logs for the mutator and CDC consumer
	docker compose logs -f connect-mutator connect-cdc-consumer

lint: ## Lint both Redpanda Connect pipelines (uses local rpk)
	rpk connect lint connect/mutator.yaml connect/cdc-consumer.yaml

psql: ## Open a SQL shell on CockroachDB (cdcbank)
	docker exec -it cf-cockroachdb cockroach sql --insecure --database=cdcbank

jobs: ## Show CockroachDB changefeed jobs
	docker exec cf-cockroachdb cockroach sql --insecure --execute="SELECT job_id, status, running_status FROM [SHOW CHANGEFEED JOBS];"

changefeed-health: ## CockroachDB-side CDC health (status + high-water + emitted metrics)
	./scripts/changefeed-health.sh

topic: ## Show the changefeed topic and consumer group
	docker exec cf-redpanda rpk topic describe accounts_cdc
	docker exec cf-redpanda rpk group describe cdc-audit
