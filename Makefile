.PHONY: test-mvp demo docker-build docker-demo clean-demo
.PHONY: realtime-up realtime-down realtime-test realtime-clean

test-mvp:
	python -m pytest tests/ -v -m "not bronze_integration and not silver_integration and not e2e"

demo:
	RISKCLOUD_ICEBERG_WAREHOUSE=$(CURDIR)/data/warehouse \
	RISKCLOUD_ARTIFACTS_DIR=$(CURDIR)/data/artifacts \
	python scripts/run_resume_demo.py

docker-build:
	docker compose build

docker-demo:
	docker compose run --rm riskcloud-demo

clean-demo:
	rm -rf data/warehouse data/artifacts

# -- Realtime infrastructure --

realtime-up:
	docker compose -f deploy/local/docker-compose.realtime.yml up -d --wait

realtime-down:
	docker compose -f deploy/local/docker-compose.realtime.yml down

realtime-test:
	python -m pytest tests/streaming/ -v

realtime-clean:
	docker compose -f deploy/local/docker-compose.realtime.yml down -v
	rm -rf data/kafka data/flink/checkpoints data/flink/savepoints
