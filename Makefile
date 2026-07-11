.PHONY: up init demo test down logs shell demo-pipeline

# Bring up the single-node stack (Redpanda, ClickHouse, MinIO, bucket, app).
up:
	docker compose up -d --build
	@echo "waiting for services to become healthy..."
	@docker compose ps

# Create the canonical demo topic + Iceberg table (CH tables come from init.sql).
init:
	docker compose exec -T app python scripts/bootstrap.py

# Phase 1 acceptance: the three correctness tests must go green.
demo test:
	docker compose exec -T app pytest -v

# Run the canonical pipeline live (generate -> land -> seam query) for a demo.
demo-pipeline:
	docker compose exec -T app python -m streaming_lab.events.generator -n 100000
	docker compose exec -T app python -m streaming_lab.lake.landing_job --idle-exit-ms 5000
	docker compose exec -T app python -m streaming_lab.query.service --once

down:
	docker compose down -v

logs:
	docker compose logs -f

shell:
	docker compose exec app bash
