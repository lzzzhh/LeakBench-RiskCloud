.PHONY: test-mvp demo docker-build docker-demo clean-demo
.PHONY: realtime-up realtime-down realtime-test realtime-smoke realtime-clean

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
REALTIME_COMPOSE := docker compose \
	--project-directory $(CURDIR) \
	-f $(CURDIR)/deploy/local/docker-compose.realtime.yml

realtime-up:
	$(REALTIME_COMPOSE) up -d --wait kafka flink-jobmanager flink-taskmanager
	$(REALTIME_COMPOSE) run --rm kafka-init

realtime-down:
	$(REALTIME_COMPOSE) down

realtime-contract-test:
	python -m pytest tests/streaming/ -v

realtime-smoke:
	bash deploy/local/smoke-test.sh

realtime-test:
	$(MAKE) realtime-contract-test
	$(MAKE) realtime-smoke

realtime-clean:
	-$(REALTIME_COMPOSE) down -v --remove-orphans
	docker run --rm -v $(CURDIR)/data:/data alpine:3.20 sh -ec 'rm -rf /data/kafka /data/flink/checkpoints /data/flink/savepoints'