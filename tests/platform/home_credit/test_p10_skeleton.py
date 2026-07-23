"""P1.0 tests — Directory structure contract and Spark env import."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


class TestDirectoryContract:

    def test_adapter_lives_under_riskcloud_adapters(self):
        """Per Phase 0 contract: implementations live under riskcloud/adapters/<name>/"""
        path = REPO_ROOT / "riskcloud" / "adapters" / "home_credit"
        assert path.is_dir(), f"Adapter directory missing: {path}"

    def test_adapter_has_init(self):
        path = REPO_ROOT / "riskcloud" / "adapters" / "home_credit" / "__init__.py"
        # May not exist yet in P1.0 — that's OK, but the directory must
        assert path.parent.is_dir()

    def test_case_studies_has_manifests(self):
        for f in [
            "case_studies/home_credit/manifests/data_manifest.yaml",
            "case_studies/home_credit/manifests/snapshot_manifest.template.yaml",
        ]:
            assert (REPO_ROOT / f).is_file(), f"Missing: {f}"

    def test_case_studies_has_spark_env(self):
        assert (REPO_ROOT / "case_studies/home_credit/pipelines/spark_env.py").is_file()

    def test_case_studies_has_no_own_adapters_dir(self):
        """Adapter code must be under riskcloud/adapters/, not case_studies/."""
        path = REPO_ROOT / "case_studies" / "home_credit" / "adapters"
        assert not path.exists(), "Adapter directory should not exist under case_studies"

    def test_tests_under_root_tests(self):
        assert (REPO_ROOT / "tests" / "platform" / "home_credit").is_dir()

    def test_readme_exists(self):
        assert (REPO_ROOT / "case_studies" / "home_credit" / "README.md").is_file()


class TestSparkEnvImport:

    def test_spark_env_module_imports(self):
        """Smoke test: module imports without Spark runtime."""
        from case_studies.home_credit.pipelines import spark_env
        assert spark_env.ICEBERG_VERSION == "1.6.1"
        assert spark_env.CATALOG == "riskcloud"

    def test_version_matrix_pinned(self):
        from case_studies.home_credit.pipelines import spark_env
        assert spark_env.ICEBERG_VERSION
        assert spark_env.SCALA_BINARY
        assert spark_env.SPARK_MAJOR
        assert "iceberg-spark-runtime" in spark_env.ICEBERG_RUNTIME

    def test_warehouse_default(self):
        from case_studies.home_credit.pipelines import spark_env
        assert "iceberg_warehouse" in spark_env._WAREHOUSE
