# Home Credit — Phase 1

## P1.0 — Data Inventory & Execution Skeleton

### Setup

```bash
# Download Home Credit data from Kaggle
# Place CSVs in a directory, e.g. ~/data/home_credit/

# Populate the data manifest
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/ \
    --populate

# Validate the manifest
python case_studies/home_credit/scripts/validate_manifest.py \
    --data-dir ~/data/home_credit/

# Install dependencies
pip install pyspark pyiceberg pyyaml

# Verify Spark/Iceberg environment
python -c "
from case_studies.home_credit.pipelines.spark_env import get_spark, setup_namespaces
spark = get_spark()
setup_namespaces(spark)
spark.sql('SHOW NAMESPACES IN riskcloud').show()
spark.stop()
"
```

### Directory Layout

```
case_studies/home_credit/
├── manifests/
│   ├── data_manifest.yaml      # Input file inventory (SHA, row counts, columns)
│   └── snapshot_manifest.yaml  # Snapshot identity (Iceberg snapshot ID, code SHA)
├── pipelines/
│   └── spark_env.py            # SparkSession builder with Iceberg
├── adapters/                   # P1.1: Home Credit Adapter
├── configs/                    # Pipeline configuration
├── scripts/
│   └── validate_manifest.py    # Manifest validation & population
└── tests/                      # P1.1+: Pipeline tests
```

### First Vertical Slice

The initial slice only requires:
- `application_train.csv`
- `bureau.csv`
- `bureau_balance.csv`

These three tables are sufficient to validate the core Phase 1 engineering risks:
1. One-to-many aggregation without sample inflation
2. Bureau history temporal boundary enforcement
3. Sub-table aggregation before joining
4. Prediction Point construction before feature building
5. Snapshot and re-run auditability
6. Strict vs. full feature view separation

After this slice passes, add `previous_application`, `installments_payments`,
`credit_card_balance`, and `POS_CASH_balance`.
