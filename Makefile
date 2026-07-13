.PHONY: help up down ps bootstrap buckets migrate smoke reliability-smoke source-check psql clean-pycache

help:
	@echo "NeuroSleep local commands"
	@echo
	@echo "make up              Start PostgreSQL and MinIO"
	@echo "make down            Stop local Docker services"
	@echo "make ps              Show Docker services"
	@echo "make bootstrap       Bootstrap the local platform"
	@echo "make buckets         Initialize MinIO buckets"
	@echo "make migrate         Run SQL migrations and seeds"
	@echo "make smoke           Run all smoke tests"
	@echo "make reliability-smoke  Run reliability and failure tests"
	@echo "make source-check    Check Sleep-EDF source configuration"
	@echo "make psql            Open PostgreSQL psql shell"
	@echo "make clean-pycache   Remove Python cache folders"

up:
	docker compose up -d postgres minio

down:
	docker compose down

ps:
	docker compose ps

bootstrap:
	./scripts/bootstrap_local.sh

buckets:
	./scripts/init_minio_buckets.sh

migrate:
	./scripts/run_sql_migrations.sh

smoke:
	./scripts/run_smoke_tests.sh

reliability-smoke:
	./scripts/run_reliability_smoke_tests.sh

source-check:
	PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf

psql:
	docker compose exec postgres psql -P pager=off -U neuro_sleep -d neuro_sleep

clean-pycache:
	find src -type d -name "__pycache__" -prune -exec rm -rf {} +
