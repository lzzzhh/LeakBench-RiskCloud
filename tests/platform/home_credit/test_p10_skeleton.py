"""P1.0 tests — Directory structure contract and Spark env import."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


class TestDirectoryContract:

    def test_adapter_package_is_tracked(self):
        """Adapter __init__.py must be a tracked file (not just empty directory)."""
        path = REPO_ROOT / "riskcloud" / "adapters" / "home_credit" / "__init__.py"
        assert path.is_file(), f"Adapter package not tracked: {path}"

    def test_case_studies_has_manifests(self):
        for f in [
            "case_studies/home_credit/manifests/data_manifest.yaml",
            "case_studies/home_credit/manifests/snapshot_manifest.template.yaml",
        ]:
            assert (REPO_ROOT / f).is_file(), f"Missing: {f}"

    def test_case_studies_has_spark_env(self):
        assert (REPO_ROOT / "case_studies/home_credit/pipelines/spark_env.py").is_file()

    def test_case_studies_has_no_own_adapters_dir(self):
        path = REPO_ROOT / "case_studies" / "home_credit" / "adapters"
        assert not path.exists()

    def test_tests_under_root_tests(self):
        assert (REPO_ROOT / "tests" / "platform" / "home_credit").is_dir()

    def test_readme_exists(self):
        assert (REPO_ROOT / "case_studies" / "home_credit" / "README.md").is_file()


class TestSparkEnvImport:

    def test_module_imports(self):
        from case_studies.home_credit.pipelines import spark_env
        assert spark_env.ICEBERG_VERSION == "1.6.1"

    def test_version_matrix_pinned(self):
        from case_studies.home_credit.pipelines import spark_env
        assert spark_env.ICEBERG_VERSION
        assert spark_env.SCALA_BINARY
        assert spark_env.SPARK_MAJOR
        assert "iceberg-spark-runtime" in spark_env.ICEBERG_RUNTIME

    def test_has_cli_main(self):
        from case_studies.home_credit.pipelines import spark_env
        assert callable(spark_env.main)


class TestGitignore:

    def test_warehouse_is_gitignored(self):
        gi = REPO_ROOT / ".gitignore"
        content = gi.read_text()
        assert "iceberg_warehouse" in content
