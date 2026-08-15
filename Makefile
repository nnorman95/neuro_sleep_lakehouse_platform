.PHONY: help up down ps bootstrap buckets migrate smoke reliability-smoke silver-smoke spark-smoke spark-feature-check gold-signal-features gold-signal-features-check gold-reliability-smoke feature-integration-check integrated-signal-features integrated-signal-features-check phase8-check test source-check psql clean-pycache

help:
	@echo "NeuroSleep local commands"
	@echo
	@echo "make up                 Start PostgreSQL and MinIO"
	@echo "make down               Stop local Docker services"
	@echo "make ps                 Show Docker services"
	@echo "make bootstrap          Bootstrap the local platform"
	@echo "make buckets            Initialize MinIO buckets"
	@echo "make migrate            Run SQL migrations and seeds"
	@echo "make smoke              Run core platform smoke tests"
	@echo "make reliability-smoke  Run reliability and failure tests"
	@echo "make silver-smoke       Run Silver-layer smoke tests"
	@echo "make spark-smoke        Run Spark smoke tests"
	@echo "make spark-feature-check Validate Spark signal features"
	@echo "make gold-signal-features Build idempotent Gold signal features"
	@echo "make gold-signal-features-check Validate Gold signal features"
	@echo "make gold-reliability-smoke Test Gold recovery and fail-closed behavior"
	@echo "make feature-integration-check Validate Gold + Warehouse feature integration"
	@echo "make integrated-signal-features Publish integrated Gold signal features"
	@echo "make integrated-signal-features-check Validate integrated Gold signal features"
	@echo "make phase8-check         Run complete Phase 8 regression"
	@echo "make test               Run all test suites"
	@echo "make source-check       Check Sleep-EDF source configuration"
	@echo "make psql               Open PostgreSQL psql shell"
	@echo "make clean-pycache      Remove Python cache folders"

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

silver-smoke:
	./scripts/run_silver_smoke_tests.sh

spark-smoke:
	./scripts/run_spark_smoke_tests.sh

spark-feature-check:
	./scripts/run_signal_feature_validation.sh

gold-signal-features:
	./scripts/run_gold_signal_features.sh

gold-signal-features-check:
	./scripts/validate_gold_signal_features.sh

gold-reliability-smoke:
	./scripts/run_gold_reliability_smoke_tests.sh

feature-integration-check:
	./scripts/run_feature_integration_validation.sh

integrated-signal-features:
	./scripts/run_integrated_signal_features.sh

integrated-signal-features-check:
	./scripts/validate_integrated_signal_features.sh

phase8-check:
	./scripts/validate_phase8.sh

test: smoke reliability-smoke silver-smoke spark-smoke gold-reliability-smoke

source-check:
	PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf

psql:
	docker compose exec postgres psql -P pager=off -U neuro_sleep -d neuro_sleep

clean-pycache:
	find src -type d -name "__pycache__" -prune -exec rm -rf {} +
