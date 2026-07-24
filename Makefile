.PHONY: test-mvp demo docker-build docker-demo clean-demo

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
