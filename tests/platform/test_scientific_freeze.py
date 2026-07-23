"""Contract hygiene & isolation tests for LeakBench-RiskCloud.

Tests:
  1. Freeze lock file exists and can be verified
  2. Contract docstrings reference design doc sections
  3. Package structure integrity
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


# -----------------------------------------------------------------
# 1. Freeze lock
# -----------------------------------------------------------------

class TestFreezeLock:

    def test_freeze_lock_exists(self):
        lock = REPO_ROOT / "scientific-freeze.lock"
        assert lock.exists(), "scientific-freeze.lock not found"

    def test_freeze_lock_valid_structure(self):
        import yaml
        lock = REPO_ROOT / "scientific-freeze.lock"
        data = yaml.safe_load(lock.read_text())
        assert "upstream" in data
        assert "repository" in data["upstream"]
        assert "commit" in data["upstream"]
        assert len(data["upstream"]["commit"]) == 40
        assert "protected" in data
        assert "path" in data["protected"]
        assert "tree_sha" in data["protected"]

    def test_freeze_verification_runs(self):
        """Freeze verification module runs without crashing (may fail if offline)."""
        from riskcloud.freeze import verify_freeze, FreezeResult
        lock = REPO_ROOT / "scientific-freeze.lock"
        result = verify_freeze(lock)
        assert isinstance(result, FreezeResult)
        assert result.report()  # string output


# -----------------------------------------------------------------
# 2. Documentation
# -----------------------------------------------------------------

class TestDocumentation:

    def test_contracts_have_docstrings(self):
        import riskcloud.contracts.event as evt
        import riskcloud.contracts.prediction_point as pp
        import riskcloud.contracts.feature_catalog as fc
        import riskcloud.contracts.document as doc
        for mod, cls_name in [
            (evt, "Event"),
            (pp, "PredictionPoint"),
            (fc, "FeatureCatalogEntry"),
            (doc, "DocumentParseResult"),
        ]:
            cls = getattr(mod, cls_name)
            assert cls.__doc__ is not None, f"{cls_name} missing docstring"
            assert len(cls.__doc__.strip()) > 0

    def test_contract_files_reference_design_doc_sections(self):
        files = [
            ("riskcloud/contracts/event.py", "Section 6.1"),
            ("riskcloud/contracts/prediction_point.py", "Section 6.2"),
            ("riskcloud/contracts/feature_catalog.py", "Section 6.3"),
            ("riskcloud/contracts/document.py", "Section 6.4"),
        ]
        for rel_path, section in files:
            content = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
            assert section in content, f"{rel_path} should mention {section}"


# -----------------------------------------------------------------
# 3. Directory structure
# -----------------------------------------------------------------

class TestDirectoryStructure:

    def test_package_structure(self):
        assert (REPO_ROOT / "riskcloud").is_dir()
        assert (REPO_ROOT / "riskcloud" / "contracts").is_dir()
        assert (REPO_ROOT / "riskcloud" / "adapters").is_dir()
        assert (REPO_ROOT / "tests").is_dir()
        assert (REPO_ROOT / "docs").is_dir()

    def test_key_files_exist(self):
        for f in ["README.md", "docs/design.md", "pyproject.toml", "scientific-freeze.lock"]:
            assert (REPO_ROOT / f).is_file(), f"Missing: {f}"

    def test_adr_exists(self):
        assert (REPO_ROOT / "docs" / "adr" / "0001-rename-platform-to-riskcloud.md").is_file()
