"""Home Credit Feature Catalog V1 — 20 features, 6 semantic groups.

All entries are PUBLISHABLE (owner, lineage, risk, semantic_group set).
"""

from __future__ import annotations

from riskcloud.contracts.feature_catalog import (
    FeatureCatalogEntry,
    FeatureStage,
    LeakageRisk,
)

FEATURES: list[FeatureCatalogEntry] = [
    # ---- Application ----
    FeatureCatalogEntry(
        feature_id="app.credit_income_ratio",
        feature_name="Credit / Income Ratio",
        entity_type="application",
        feature_group="application_affordability",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_affordability",
        cost_unit=1.0,
        lineage_expression=(
            "CASE WHEN AMT_INCOME_TOTAL IS NULL OR AMT_INCOME_TOTAL=0 "
            "OR AMT_CREDIT IS NULL THEN NULL "
            "ELSE AMT_CREDIT / AMT_INCOME_TOTAL END"
        ),
        description="Ratio of loan amount to declared income (null if denominator zero/missing)",
    ),
    FeatureCatalogEntry(
        feature_id="app.annuity_income_ratio",
        feature_name="Annuity / Income Ratio",
        entity_type="application",
        feature_group="application_affordability",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_affordability",
        cost_unit=1.0,
        lineage_expression=(
            "CASE WHEN AMT_INCOME_TOTAL IS NULL OR AMT_INCOME_TOTAL=0 "
            "OR AMT_ANNUITY IS NULL THEN NULL "
            "ELSE AMT_ANNUITY / AMT_INCOME_TOTAL END"
        ),
        description="Ratio of loan annuity to declared income (null if denominator zero/missing)",
    ),
    FeatureCatalogEntry(
        feature_id="app.credit_annuity_ratio",
        feature_name="Credit / Annuity Ratio",
        entity_type="application",
        feature_group="application_affordability",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_affordability",
        cost_unit=1.0,
        lineage_expression=(
            "CASE WHEN AMT_ANNUITY IS NULL OR AMT_ANNUITY=0 "
            "OR AMT_CREDIT IS NULL THEN NULL "
            "ELSE AMT_CREDIT / AMT_ANNUITY END"
        ),
        description="Ratio of loan amount to loan annuity (null if denominator zero/missing)",
    ),
    FeatureCatalogEntry(
        feature_id="app.goods_credit_ratio",
        feature_name="Goods Price / Credit Ratio",
        entity_type="application",
        feature_group="application_affordability",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_affordability",
        cost_unit=1.0,
        lineage_expression=(
            "CASE WHEN AMT_CREDIT IS NULL OR AMT_CREDIT=0 "
            "OR AMT_GOODS_PRICE IS NULL THEN NULL "
            "ELSE AMT_GOODS_PRICE / AMT_CREDIT END"
        ),
        description="Ratio of goods price to loan amount (null if denominator zero/missing)",
    ),
    FeatureCatalogEntry(
        feature_id="app.age_years",
        feature_name="Applicant Age (Years)",
        entity_type="application",
        feature_group="application_demographics",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_demographics",
        cost_unit=1.0,
        lineage_expression="DAYS_BIRTH / -365.25",
        description="Applicant age in years (derived from DAYS_BIRTH)",
    ),
    FeatureCatalogEntry(
        feature_id="app.ext_source_mean",
        feature_name="External Source Mean",
        entity_type="application",
        feature_group="application_external_scores",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_external_scores",
        cost_unit=1.0,
        lineage_expression=(
            "(COALESCE(EXT_SOURCE_1,EXT_SOURCE_2,EXT_SOURCE_3)+"
            "COALESCE(EXT_SOURCE_2,EXT_SOURCE_3,EXT_SOURCE_1)+"
            "COALESCE(EXT_SOURCE_3,EXT_SOURCE_1,EXT_SOURCE_2))"
            "/NULLIF((CASE WHEN EXT_SOURCE_1 IS NOT NULL THEN 1 ELSE 0 END+"
            "CASE WHEN EXT_SOURCE_2 IS NOT NULL THEN 1 ELSE 0 END+"
            "CASE WHEN EXT_SOURCE_3 IS NOT NULL THEN 1 ELSE 0 END),0)"
        ),
        description="Mean of available external credit scores (null if all three missing)",
    ),
    FeatureCatalogEntry(
        feature_id="app.ext_source_missing_count",
        feature_name="External Source Missing Count",
        entity_type="application",
        feature_group="application_external_scores",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_external_scores",
        cost_unit=1.0,
        lineage_expression=(
            "CASE WHEN EXT_SOURCE_1 IS NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_2 IS NULL THEN 1 ELSE 0 END + "
            "CASE WHEN EXT_SOURCE_3 IS NULL THEN 1 ELSE 0 END"
        ),
        description="Number of missing external credit scores (0-3)",
    ),
    FeatureCatalogEntry(
        feature_id="app.document_flag_count",
        feature_name="Document Flag Count",
        entity_type="application",
        feature_group="application_documents",
        source_system="application_train",
        event_time_rule="application snapshot at prediction_time",
        availability_rule="available from application snapshot at prediction_time",
        stage=FeatureStage.APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.NONE,
        semantic_group_id="application_documents",
        cost_unit=1.0,
        lineage_expression="FLAG_DOCUMENT_2+FLAG_DOCUMENT_3+FLAG_DOCUMENT_4+FLAG_DOCUMENT_5+FLAG_DOCUMENT_6+FLAG_DOCUMENT_7+FLAG_DOCUMENT_8+FLAG_DOCUMENT_9+FLAG_DOCUMENT_10+FLAG_DOCUMENT_11+FLAG_DOCUMENT_12+FLAG_DOCUMENT_13+FLAG_DOCUMENT_14+FLAG_DOCUMENT_15+FLAG_DOCUMENT_16+FLAG_DOCUMENT_17+FLAG_DOCUMENT_18+FLAG_DOCUMENT_19+FLAG_DOCUMENT_20+FLAG_DOCUMENT_21",
        description="Sum of document submission flags (FLAG_DOCUMENT_2 through FLAG_DOCUMENT_21)",
    ),
    # ---- Bureau ----
    FeatureCatalogEntry(
        feature_id="bureau.record_count",
        feature_name="Bureau Record Count",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE DAYS_CREDIT <= 0",
        description="Total number of historical credit bureau records",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.active_count",
        feature_name="Active Bureau Count",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE DAYS_CREDIT <= 0 AND CREDIT_ACTIVE='Active'",
        description="Number of currently active bureau credits",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.closed_count",
        feature_name="Closed Bureau Count",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE DAYS_CREDIT <= 0 AND CREDIT_ACTIVE='Closed'",
        description="Number of closed bureau credits",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.credit_sum_total",
        feature_name="Bureau Credit Sum Total",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="sum(AMT_CREDIT_SUM) WHERE DAYS_CREDIT <= 0",
        description="Total sum of credit amounts across bureau records",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.debt_sum_total",
        feature_name="Bureau Debt Sum Total",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="sum(AMT_CREDIT_SUM_DEBT) WHERE DAYS_CREDIT <= 0",
        description="Total sum of current debt across bureau records",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.overdue_sum_total",
        feature_name="Bureau Overdue Sum Total",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="sum(AMT_CREDIT_SUM_OVERDUE) WHERE DAYS_CREDIT <= 0",
        description="Total sum of overdue amounts across bureau records",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.days_credit_mean",
        feature_name="Bureau Mean Days Credit",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="mean(DAYS_CREDIT) WHERE DAYS_CREDIT <= 0",
        description="Mean days since credit was opened",
    ),
    FeatureCatalogEntry(
        feature_id="bureau.recent_12m_count",
        feature_name="Bureau Recent 12-Month Count",
        entity_type="application",
        feature_group="bureau_credit_history",
        source_system="bureau",
        event_time_rule="bureau record with DAYS_CREDIT <= 0",
        availability_rule="historical bureau snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_credit_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE DAYS_CREDIT BETWEEN -365 AND 0",
        description="Number of bureau records in the last 12 months",
    ),
    # ---- Bureau Balance ----
    FeatureCatalogEntry(
        feature_id="bureau_balance.month_count",
        feature_name="Bureau Balance Month Count",
        entity_type="application",
        feature_group="bureau_delinquency_history",
        source_system="bureau_balance",
        event_time_rule="bureau_balance month with MONTHS_BALANCE <= 0",
        availability_rule="enriched bureau balance snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_delinquency_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE MONTHS_BALANCE <= 0",
        description="Number of monthly bureau balance snapshots",
    ),
    FeatureCatalogEntry(
        feature_id="bureau_balance.delinquent_month_count",
        feature_name="Delinquent Month Count",
        entity_type="application",
        feature_group="bureau_delinquency_history",
        source_system="bureau_balance",
        event_time_rule="bureau_balance month with MONTHS_BALANCE <= 0",
        availability_rule="enriched bureau balance snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_delinquency_history",
        cost_unit=1.0,
        lineage_expression="count(*) WHERE STATUS IN ('1','2','3','4','5') AND MONTHS_BALANCE <= 0",
        description="Number of months with delinquency status",
    ),
    FeatureCatalogEntry(
        feature_id="bureau_balance.max_delinquency_level",
        feature_name="Max Delinquency Level",
        entity_type="application",
        feature_group="bureau_delinquency_history",
        source_system="bureau_balance",
        event_time_rule="bureau_balance month with MONTHS_BALANCE <= 0",
        availability_rule="enriched bureau balance snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_delinquency_history",
        cost_unit=1.0,
        lineage_expression=(
            "MAX(CASE STATUS WHEN '0' THEN 0 WHEN '1' THEN 1 WHEN '2' THEN 2 "
            "WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5 "
            "WHEN 'C' THEN 0 WHEN 'X' THEN NULL ELSE NULL END) "
            "WHERE MONTHS_BALANCE <= 0"
        ),
        description="Maximum delinquency status level across all months",
    ),
    FeatureCatalogEntry(
        feature_id="bureau_balance.latest_status_delinquent",
        feature_name="Latest Status Delinquent",
        entity_type="application",
        feature_group="bureau_delinquency_history",
        source_system="bureau_balance",
        event_time_rule="bureau_balance month with MONTHS_BALANCE <= 0",
        availability_rule="enriched bureau balance snapshot available at prediction_time",
        stage=FeatureStage.PRE_APPLICATION,
        owner="riskcloud.home_credit",
        version=1,
        leakage_risk=LeakageRisk.TEMPORAL,
        semantic_group_id="bureau_delinquency_history",
        cost_unit=1.0,
        lineage_expression=(
            "SELECT CASE WHEN STATUS IN ('1','2','3','4','5') THEN 1 ELSE 0 END "
            "FROM bureau_balance WHERE SK_ID_CURR=t.SK_ID_CURR AND MONTHS_BALANCE<=0 "
            "ORDER BY MONTHS_BALANCE DESC, SK_ID_BUREAU ASC LIMIT 1"
        ),
        description="Whether the most recent month per applicant shows delinquency",
    ),
]


def get_features() -> list[FeatureCatalogEntry]:
    return list(FEATURES)


SEMANTIC_GROUPS: dict[str, str] = {}
for f in FEATURES:
    SEMANTIC_GROUPS[f.feature_id] = f.semantic_group_id or ""


def get_semantic_group_mapping() -> dict[str, str]:
    return dict(SEMANTIC_GROUPS)
