# Home Credit — Phase 1

## P1.0 — Data Inventory & Execution Skeleton

### Compatibility Matrix

| Component | Version |
|---|---|
| PySpark | 3.5.3 |
| Iceberg (Spark runtime) | 1.6.1 |
| Scala binary | 2.12 |
| Java | 17 |
| Python | >= 3.10 |

### Setup

```bash
# 1. Download Home Credit data from Kaggle
#    Place CSVs in a directory, e.g. ~/data/home_credit/

# 2. Populate the data manifest (validates before saving)
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/ \
    --populate

# 3. Run the Spark/Iceberg smoke test (requires Java 17)
python -m case_studies.home_credit.pipelines.spark_env

# 4. Run all tests
python -m pytest tests/ -v
```

### Directory Layout (Phase 0 contract compliant)

```
riskcloud/
├── adapters/
│   └── home_credit/              # P1.1: Adapter
│       └── __init__.py

case_studies/home_credit/
├── manifests/
│   ├── data_manifest.yaml            # Input file inventory (populate with --populate)
│   └── snapshot_manifest.template.yaml  # Run identity template
├── pipelines/
│   └── spark_env.py                  # SparkSession + Iceberg + smoke test + CLI
├── scripts/
│   └── validate_manifest.py          # Manifest validation & population
└── README.md

tests/
├── fixtures/home_credit/             # Toy CSVs for unit tests
├── platform/home_credit/
│   ├── test_manifest.py
│   ├── test_p10_skeleton.py
│   └── test_snapshot_template.py
```

### First Vertical Slice

Requires: `application_train.csv`, `bureau.csv`, `bureau_balance.csv`
