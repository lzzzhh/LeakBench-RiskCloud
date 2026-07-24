"""P1.5 — Feature catalog closure test (pure Python, no Spark)."""

from riskcloud.adapters.home_credit.feature_catalog import get_features


class TestFeatureCatalogClosure:

    def test_exactly_20_features(self):
        features = get_features()
        assert len(features) == 20, f"Expected 20 features, got {len(features)}"

    def test_all_publishable(self):
        for f in get_features():
            assert f.is_publishable(), f"{f.feature_id}: {f.publishable_errors()}"

    def test_app_bureau_balance_all_present(self):
        ids = {f.feature_id for f in get_features()}
        app = {f for f in ids if f.startswith("app.")}
        bur = {f for f in ids if f.startswith("bureau.") and not f.startswith("bureau_balance.")}
        bub = {f for f in ids if f.startswith("bureau_balance.")}
        assert len(app) == 8, f"Expected 8 app features, got {len(app)}: {app}"
        assert len(bur) == 8, f"Expected 8 bureau features, got {len(bur)}: {bur}"
        assert len(bub) == 4, f"Expected 4 bureau_balance features, got {len(bub)}: {bub}"

    def test_no_target_in_catalog(self):
        for f in get_features():
            assert "TARGET" not in f.feature_id.upper()
            assert "TARGET" not in f.feature_name.upper()
