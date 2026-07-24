"""Feature Computation Registry — batch and stream specs for all 20 features.

Every feature must have batch_implementation_ref and stream_expression_sql.
Closure validation ensures 20/20 coverage with temporal filters preserved.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from riskcloud.adapters.home_credit.feature_catalog import get_features


class ComputationKind(str, Enum):
    ROW = "ROW"
    AGGREGATE = "AGGREGATE"
    LATEST = "LATEST"


@dataclass(frozen=True)
class FeatureComputationSpec:
    feature_id: str
    source_table: str
    computation_kind: ComputationKind
    group_key: str | None
    batch_implementation_ref: str
    stream_expression_sql: str
    input_filter_sql: str | None = None
    output_type: str = "DOUBLE"
    batch_enabled: bool = True
    stream_enabled: bool = True
    online_enabled: bool = True
    freshness_sla_seconds: int = 60
    null_semantics: str = "NULL_IF_MISSING"


def build_computation_specs() -> list[FeatureComputationSpec]:
    features = get_features()
    # Specs are built from the catalog — every feature must be mapped
    app_specs = _build_app_specs([f for f in features if f.feature_id.startswith("app.")])
    bur_specs = _build_bureau_specs(
        [f for f in features if f.feature_id.startswith("bureau.") and not f.feature_id.startswith("bureau_balance.")]
    )
    bub_specs = _build_bub_specs([f for f in features if f.feature_id.startswith("bureau_balance.")])
    return app_specs + bur_specs + bub_specs


def _build_app_specs(features) -> list[FeatureComputationSpec]:
    sql_map = _app_sql()
    return [
        FeatureComputationSpec(
            feature_id=f.feature_id,
            source_table="application_train",
            computation_kind=ComputationKind.ROW,
            group_key=None,
            batch_implementation_ref="case_studies.home_credit.pipelines.features:compute_features",
            stream_expression_sql=sql_map[f.feature_id],
        )
        for f in features
    ]


def _build_bureau_specs(features) -> list[FeatureComputationSpec]:
    sql_map = _bureau_sql()
    return [
        FeatureComputationSpec(
            feature_id=f.feature_id,
            source_table="bureau",
            computation_kind=ComputationKind.AGGREGATE,
            group_key="SK_ID_CURR",
            batch_implementation_ref="case_studies.home_credit.pipelines.features:compute_features",
            stream_expression_sql=sql_map[f.feature_id],
            input_filter_sql="DAYS_CREDIT <= 0",
        )
        for f in features
    ]


def _build_bub_specs(features) -> list[FeatureComputationSpec]:
    sql_map = _bub_sql()
    kinds = {
        "bureau_balance.month_count": ComputationKind.AGGREGATE,
        "bureau_balance.delinquent_month_count": ComputationKind.AGGREGATE,
        "bureau_balance.max_delinquency_level": ComputationKind.AGGREGATE,
        "bureau_balance.latest_status_delinquent": ComputationKind.LATEST,
    }
    return [
        FeatureComputationSpec(
            feature_id=f.feature_id,
            source_table="bureau_balance",
            computation_kind=kinds.get(f.feature_id, ComputationKind.AGGREGATE),
            group_key="SK_ID_CURR",
            batch_implementation_ref="case_studies.home_credit.pipelines.features:compute_features",
            stream_expression_sql=sql_map[f.feature_id],
            input_filter_sql="MONTHS_BALANCE <= 0",
        )
        for f in features
    ]


def _app_sql() -> dict[str, str]:
    return {
        "app.credit_income_ratio": "CAST(AMT_CREDIT AS DOUBLE) / NULLIF(CAST(AMT_INCOME_TOTAL AS DOUBLE), 0)",
        "app.annuity_income_ratio": "CAST(AMT_ANNUITY AS DOUBLE) / NULLIF(CAST(AMT_INCOME_TOTAL AS DOUBLE), 0)",
        "app.credit_annuity_ratio": "CAST(AMT_CREDIT AS DOUBLE) / NULLIF(CAST(AMT_ANNUITY AS DOUBLE), 0)",
        "app.goods_credit_ratio": "CAST(AMT_GOODS_PRICE AS DOUBLE) / NULLIF(CAST(AMT_CREDIT AS DOUBLE), 0)",
        "app.age_years": "CAST(DAYS_BIRTH AS DOUBLE) / -365.25",
        "app.ext_source_mean": (
            "(COALESCE(CAST(EXT_SOURCE_1 AS DOUBLE), 0) + COALESCE(CAST(EXT_SOURCE_2 AS DOUBLE), 0) + "
            "COALESCE(CAST(EXT_SOURCE_3 AS DOUBLE), 0)) / NULLIF(CAST("
            "CASE WHEN EXT_SOURCE_1 IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_2 IS NOT NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_3 IS NOT NULL THEN 1 ELSE 0 END AS DOUBLE), 0)"
        ),
        "app.ext_source_missing_count": (
            "CAST(CASE WHEN EXT_SOURCE_1 IS NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_2 IS NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_3 IS NULL THEN 1 ELSE 0 END AS DOUBLE)"
        ),
        "app.document_flag_count": (
            "CAST(FLAG_DOCUMENT_2+FLAG_DOCUMENT_3+FLAG_DOCUMENT_4+FLAG_DOCUMENT_5+"
            "FLAG_DOCUMENT_6+FLAG_DOCUMENT_7+FLAG_DOCUMENT_8+FLAG_DOCUMENT_9+"
            "FLAG_DOCUMENT_10+FLAG_DOCUMENT_11+FLAG_DOCUMENT_12+FLAG_DOCUMENT_13+"
            "FLAG_DOCUMENT_14+FLAG_DOCUMENT_15+FLAG_DOCUMENT_16+FLAG_DOCUMENT_17+"
            "FLAG_DOCUMENT_18+FLAG_DOCUMENT_19+FLAG_DOCUMENT_20+FLAG_DOCUMENT_21 AS DOUBLE)"  # noqa: E501
        ),
    }


def _bureau_sql() -> dict[str, str]:
    return {
        "bureau.record_count": "COUNT(*)",
        "bureau.active_count": "SUM(CASE WHEN CREDIT_ACTIVE = 'Active' THEN 1 ELSE 0 END)",
        "bureau.closed_count": "SUM(CASE WHEN CREDIT_ACTIVE = 'Closed' THEN 1 ELSE 0 END)",
        "bureau.credit_sum_total": "SUM(CAST(AMT_CREDIT_SUM AS DOUBLE))",
        "bureau.debt_sum_total": "SUM(CAST(AMT_CREDIT_SUM_DEBT AS DOUBLE))",
        "bureau.overdue_sum_total": "SUM(CAST(AMT_CREDIT_SUM_OVERDUE AS DOUBLE))",
        "bureau.days_credit_mean": "AVG(CAST(DAYS_CREDIT AS DOUBLE))",
        "bureau.recent_12m_count": "SUM(CASE WHEN DAYS_CREDIT BETWEEN -365 AND 0 THEN 1 ELSE 0 END)",
    }


def _bub_sql() -> dict[str, str]:
    return {
        "bureau_balance.month_count": "COUNT(*)",
        "bureau_balance.delinquent_month_count": "SUM(CASE WHEN STATUS IN ('1','2','3','4','5') THEN 1 ELSE 0 END)",
        "bureau_balance.max_delinquency_level": (
            "MAX(CASE STATUS WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2 "
            "WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5 WHEN 'C' THEN 0 WHEN 'X' THEN 0 END)"
        ),
        "bureau_balance.latest_status_delinquent": (
            "CAST(FIRST_VALUE(CASE WHEN STATUS IN ('1','2','3','4','5') THEN 1 ELSE 0 END) "
            "OVER (PARTITION BY SK_ID_CURR ORDER BY MONTHS_BALANCE DESC, SK_ID_BUREAU ASC) AS DOUBLE)"
        ),
    }


def validate_closure() -> tuple[bool, list[str]]:
    features = get_features()
    specs = build_computation_specs()
    cat_ids = {f.feature_id for f in features}
    spec_ids = {s.feature_id for s in specs}
    errors = []

    missing = cat_ids - spec_ids
    if missing:
        errors.append(f"Missing specs for: {sorted(missing)}")
    extra = spec_ids - cat_ids
    if extra:
        errors.append(f"Extra specs not in catalog: {sorted(extra)}")

    for s in specs:
        if not s.batch_implementation_ref:
            errors.append(f"{s.feature_id}: batch_implementation_ref empty")
        if not s.stream_expression_sql:
            errors.append(f"{s.feature_id}: stream_expression_sql empty")
        if s.source_table not in ("application_train", "bureau", "bureau_balance"):
            errors.append(f"{s.feature_id}: unknown source_table {s.source_table}")
        if s.source_table in ("bureau", "bureau_balance") and not s.input_filter_sql:
            errors.append(f"{s.feature_id}: missing temporal filter")

    return len(errors) == 0, errors
