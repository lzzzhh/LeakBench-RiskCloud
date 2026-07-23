# Home Credit — Phase 1

## P1.0 — Data Inventory & Execution Skeleton (AUDIT OPEN)

### Setup

```bash
# 1. Download Home Credit data from Kaggle
#    Place CSVs in a directory, e.g. ~/data/home_credit/

# 2. Populate the data manifest
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/ \
    --populate

# 3. Validate the manifest
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/

# 4. Install dependencies
pip install pyspark==3.5.* pyyaml

# 5. Run the Spark/Iceberg smoke test
python -m case_studies.home_credit.pipelines.spark_env  # (if __main__ added)
```

### Compatiblity Matrix

| Component | Version |
|---|---|
| PySpark | 3.5 |
| Iceberg (Spark runtime) | 1.6.1 |
| Scala binary | 2.12 |
| Java | 11 or 17 |
| Python | >= 3.10 |

### Directory Layout (Phase 0 contract compliant)

```
riskcloud/
├── adapters/
│   └── home_credit/              # P1.1: Adapter (per Phase 0 contract)
│       ├── __init__.py
│       ├── adapter.py
│       ├── boundary.py
│       ├── field_mapping.py
│       └── feature_catalog.py

case_studies/home_credit/
├── manifests/
│   ├── data_manifest.yaml            # Input file inventory
│   └── snapshot_manifest.template.yaml  # Run identity template
├── pipelines/
│   └── spark_env.py                  # SparkSession + Iceberg + smoke test
├── configs/
├── scripts/
│   └── validate_manifest.py          # Manifest validation & population
└── README.md

tests/platform/home_credit/
├── test_manifest.py
├── test_adapter.py                   # P1.1+
├── test_boundary.py                  # P1.1+
├── test_feature_catalog.py           # P1.1+
└── test_spark_iceberg_smoke.py
```

### First Vertical Slice

The initial slice only requires:
- `application_train.csv`
- `bureau.csv`
- `bureau_balance.csv`

After this slice passes, add `previous_application`, `installments_payments`,
`credit_card_balance`, and `POS_CASH_balance`.
