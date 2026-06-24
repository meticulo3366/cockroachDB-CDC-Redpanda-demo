# Turnkey targets for the Redpanda -> CockroachDB streaming demo.
.PHONY: help up down verify status watch connectors logs lint psql topic clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Start the stack and wait until healthy
	./scripts/up.sh

verify: ## Verify data is flowing synthetic -> Redpanda -> CockroachDB
	./scripts/verify.sh

status: ## One snapshot: is Redpanda producing & CockroachDB ingesting?
	./scripts/status.sh

watch: ## Live status dashboard, refreshing every few seconds (Ctrl-C to stop)
	./scripts/status.sh --watch

connectors: ## Show health/throughput of both Redpanda Connect pipelines
	./scripts/connectors.sh

down: ## Stop the stack and remove volumes
	./scripts/down.sh

clean: down ## Alias for down

logs: ## Tail logs for the Connect pipelines
	docker compose logs -f connect-generator connect-sink

lint: ## Lint both Redpanda Connect pipelines (uses local rpk)
	rpk connect lint connect/generator.yaml connect/sink.yaml

psql: ## Open a SQL shell on CockroachDB
	docker exec -it cockroachdb cockroach sql --insecure --database=cdcdemo

topic: ## Show the orders topic and consumer group
	docker exec redpanda rpk topic describe orders
	docker exec redpanda rpk group describe cockroach-sink
