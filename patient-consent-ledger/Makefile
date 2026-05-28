.PHONY: help up down logs ps build seed-bq simulate clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up:  ## Boot the local stack
	docker compose up -d
	@echo ""
	@echo "Stack starting. Useful URLs:"
	@echo "  Redpanda Console: http://localhost:8080"
	@echo "  Airflow UI:       http://localhost:8081 (admin/admin)"
	@echo "  FastAPI docs:     http://localhost:8000/docs"
	@echo "  Dash:             http://localhost:8050"

down:  ## Tear down all containers (keep volumes)
	docker compose down

logs:  ## Tail logs from all services
	docker compose logs -f --tail=100

ps:  ## Show container status
	docker compose ps

build:  ## Rebuild all images
	docker compose build

seed-bq:  ## Create BigQuery sandbox datasets (raw/staging/marts/dq)
	docker compose --profile init run --rm bq-init

simulate:  ## Start the event simulator (Phase 1 — not implemented yet)
	@echo "TODO: implemented in Phase 1"

clean:  ## DESTROYS local data: tears down + removes volumes
	docker compose down -v
