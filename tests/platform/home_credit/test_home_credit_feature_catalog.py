"""P1.1 — Home Credit Feature Catalog tests."""

from __future__ import annotations

from riskcloud.adapters.home_credit.feature_catalog import (
    FEATURES,
    SEMANTIC_GROUPS,
    get_features,
    get_semantic_group_mapping,
)
from riskcloud.contracts.feature_catalog import FeatureStage, LeakageRisk


class TestCatalogStructure:

    def test_exactly_20_features(self):
        assert len(FEATURES) == 20

    def test_no_duplicate_ids(self):
        ids = [f.feature_id for f in FEATURES]
        assert len(ids) == len(set(ids))

    def test_all_publishable(self):
        for f in FEATURES:
            assert f.is_publishable(), f"{f.feature_id}: {f.publishable_errors()}"

    def test_no_target_in_catalog(self):
        for f in FEATURES:
            assert "TARGET" not in f.feature_id.upper()
            assert "TARGET" not in f.feature_name.upper()

    def test_semantic_mapping_closure(self):
        catalog_ids = {f.feature_id for f in FEATURES}
        mapping_ids = set(SEMANTIC_GROUPS.keys())
        assert catalog_ids == mapping_ids

    def test_catalog_semantic_group_matches_mapping(self):
        for f in FEATURES:
            assert f.semantic_group_id == SEMANTIC_GROUPS[f.feature_id]

    def test_stage_correct(self):
        for f in FEATURES:
            if f.feature_id.startswith("app."):
                assert f.stage == FeatureStage.APPLICATION, f.feature_id
            else:
                assert f.stage == FeatureStage.PRE_APPLICATION, f.feature_id

    def test_leakage_risk_correct(self):
        for f in FEATURES:
            if f.feature_id.startswith("app."):
                assert f.leakage_risk == LeakageRisk.NONE, f.feature_id
            else:
                assert f.leakage_risk == LeakageRisk.TEMPORAL, f.feature_id

    def test_no_post_outcome_or_label_derived(self):
        for f in FEATURES:
            assert f.stage not in (FeatureStage.POST_OUTCOME, FeatureStage.LABEL_DERIVED), f.feature_id
            assert f.leakage_risk not in (LeakageRisk.POST_OUTCOME, LeakageRisk.LABEL_DERIVED), f.feature_id

    def test_all_lineage_non_empty(self):
        for f in FEATURES:
            assert f.lineage_expression and f.lineage_expression.strip(), f.feature_id

    def test_delinquency_lineage_explicit(self):
        max_del = next(f for f in FEATURES if f.feature_id == "bureau_balance.max_delinquency_level")
        assert "CASE" in max_del.lineage_expression.upper()
        latest = next(f for f in FEATURES if f.feature_id == "bureau_balance.latest_status_delinquent")
        assert "PARTITION BY" in latest.lineage_expression.upper()
        assert "FIRST_VALUE" in latest.lineage_expression.upper()

    def test_ext_source_mean_fixture_truth_table(self):
        """Verify the lineage expression semantics via truth table reasoning."""
        # The expression is:
        # (COALESCE(E1,0)+COALESCE(E2,0)+COALESCE(E3,0))
        # / NULLIF(isNotNull(E1)+isNotNull(E2)+isNotNull(E3), 0)
        line = next(f for f in FEATURES if f.feature_id == "app.ext_source_mean")
        expr = line.lineage_expression
        assert "COALESCE(EXT_SOURCE_1,0)" in expr
        assert "COALESCE(EXT_SOURCE_2,0)" in expr
        assert "COALESCE(EXT_SOURCE_3,0)" in expr
        assert "NULLIF" in expr

    def test_latest_status_is_self_contained(self):
        """latest_status_delinquent must not reference undefined aliases like 't'."""
        latest = next(f for f in FEATURES if f.feature_id == "bureau_balance.latest_status_delinquent")
        expr = latest.lineage_expression
        assert "PARTITION BY SK_ID_CURR" in expr
        assert "FIRST_VALUE" in expr
        # Must not contain undefined alias
        assert "t.SK_ID_CURR" not in expr
        assert "t." not in expr

    def test_get_features_returns_copy(self):
        f1 = get_features()
        f2 = get_features()
        assert f1 is not f2
        assert len(f1) == len(f2)

    def test_get_semantic_mapping_returns_copy(self):
        m1 = get_semantic_group_mapping()
        m2 = get_semantic_group_mapping()
        assert m1 is not m2
        assert m1 == m2
