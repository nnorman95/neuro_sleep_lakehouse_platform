.PHONY: help up down ps bootstrap buckets migrate smoke reliability-smoke silver-smoke spark-smoke spark-feature-check gold-signal-features gold-signal-features-check gold-reliability-smoke feature-integration-check integrated-signal-features integrated-signal-features-check integrated-gold-reliability-smoke phase8-check phase9-check phase10-check test source-check psql clean-pycache kafka-up kafka-down kafka-ps kafka-smoke airflow-bootstrap airflow-up airflow-down airflow-ps airflow-smoke airflow-password
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
	@echo "make integrated-gold-reliability-smoke Test integrated Gold recovery and fail-closed behavior"
	@echo "make phase8-check         Run complete Phase 8 regression"
	@echo "make phase9-check         Run complete Phase 9 regression"
	@echo "make phase10-check        Run complete Phase 10 regression"
	@echo "make kafka-up             Start local Kafka broker"
	@echo "make kafka-down           Stop local Kafka broker"
	@echo "make kafka-ps             Show Kafka broker status"
	@echo "make kafka-smoke          Validate Kafka runtime"
	@echo "make airflow-bootstrap    Initialize and start local Airflow"
	@echo "make airflow-up           Start initialized Airflow services"
	@echo "make airflow-down         Stop Airflow services only"
	@echo "make airflow-ps           Show Airflow service status"
	@echo "make airflow-smoke        Run Airflow foundation smoke checks"
	@echo "make airflow-password     Show local Airflow admin password"
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
integrated-gold-reliability-smoke:
	./scripts/run_integrated_gold_reliability_smoke_tests.sh

phase8-check:
	./scripts/validate_phase8.sh

phase9-check:
	./scripts/validate_phase9.sh

phase10-check:
	./scripts/validate_phase10.sh

test: smoke reliability-smoke silver-smoke spark-smoke gold-reliability-smoke integrated-gold-reliability-smoke

source-check:
	PYTHONPATH=src python -m neuro_sleep.sources.sleep_edf

psql:
	docker compose exec postgres psql -P pager=off -U neuro_sleep -d neuro_sleep
clean-pycache:
	find src -type d -name "__pycache__" -prune -exec rm -rf {} +

kafka-up:
	docker compose up -d kafka

kafka-down:
	docker compose stop kafka

kafka-ps:
	docker compose ps kafka

kafka-smoke:
	./scripts/validate_kafka_runtime.sh

airflow-bootstrap:
	./scripts/bootstrap_airflow_local.sh

airflow-up:
	./scripts/airflow_compose.sh up -d --no-deps airflow-scheduler airflow-dag-processor airflow-api-server

airflow-down:
	./scripts/stop_airflow_local.sh

airflow-ps:
	./scripts/airflow_compose.sh ps

airflow-smoke:
	./scripts/run_airflow_foundation_smoke.sh

airflow-password:
	./scripts/show_airflow_password.sh
