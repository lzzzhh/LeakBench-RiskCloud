# Home Credit — Phase 1

## P1.1 — Adapter & Prediction Boundary

### Compatibility Matrix

| Component | Version |
|---|---|
| PySpark | 3.5.3 |
| Iceberg (Spark runtime) | 1.6.1 |
| Scala binary | 2.12 |
| Java | 17 |
| Python | >= 3.10 |

### Quick Start

```bash
# 1. Populate data manifest
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/ --populate

# 2. Run Spark/Iceberg smoke test
python -m case_studies.home_credit.pipelines.spark_env

# 3. Run all tests
python -m pytest tests/ -v
```

### Adapter

```python
from pathlib import Path
from datetime import datetime, timezone
from riskcloud.adapters.home_credit.adapter import HomeCreditAdapter
from riskcloud.adapters.home_credit.boundary import HomeCreditBoundaryConfig

config = HomeCreditBoundaryConfig.from_yaml("case_studies/home_credit/configs/boundary_v1.yaml")
adapter = HomeCreditAdapter(
    snapshot_id="snap-001",
    manifest_path=Path("case_studies/home_credit/manifests/data_manifest.yaml"),
    data_dir=Path("~/data/home_credit").expanduser(),
    ingested_at=datetime.now(timezone.utc),
    boundary_config=config,
)

# Validate closure
assert adapter.validate_adapter() == []
```

### Directory Layout

```
riskcloud/adapters/home_credit/
├── __init__.py
├── adapter.py              # HomeCreditAdapter
├── boundary.py             # Prediction boundary, split, label
├── field_mapping.py        # ID normalization, table constants
└── feature_catalog.py      # 20 features, 6 semantic groups

case_studies/home_credit/
├── configs/
│   └── boundary_v1.yaml    # Boundary V1 config
├── manifests/
│   ├── data_manifest.yaml
│   └── snapshot_manifest.template.yaml
├── pipelines/
│   └── spark_env.py
└── scripts/
    └── validate_manifest.py

docs/adr/
└── 0002-home-credit-proxy-time-boundary.md

tests/platform/home_credit/
├── test_home_credit_adapter.py
├── test_home_credit_boundary.py
├── test_home_credit_events.py
├── test_home_credit_feature_catalog.py
└── test_home_credit_manifest_binding.py
```
