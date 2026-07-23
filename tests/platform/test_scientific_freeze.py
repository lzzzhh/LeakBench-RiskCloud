"""Contract & isolation guard tests for LeakBench-RiskCloud.

Verify:
  1. RiskCloud contracts import cleanly (no extraneous dependencies)
  2. All contracts have docstrings and reference their design doc sections
  3. Adapter base has no hard dependency on external frameworks
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


# -----------------------------------------------------------------
# 1. Import isolation
# -----------------------------------------------------------------

class TestImportIsolation:

    def test_contracts_import_cleanly(self):
        """Contract modules must have no external framework dependencies."""
        before = set(sys.modules.keys())
        import riskcloud.contracts.event  # noqa: F401
        import riskcloud.contracts.prediction_point  # noqa: F401
        import riskcloud.contracts.feature_catalog  # noqa: F401
        import riskcloud.contracts.document  # noqa: F401
        after = set(sys.modules.keys())
        new_mods = after - before
        # Contracts should only depend on stdlib modules
        forbidden = [m for m in new_mods if any(
            m.startswith(p) for p in ("pandas", "numpy", "sklearn", "torch", "tensorflow")
        )]
        assert not forbidden, f"Contracts pulled in heavy dependencies: {forbidden}"

    def test_adapter_base_imports_cleanly(self):
        """Adapter base must not pull in heavy frameworks."""
        before = set(sys.modules.keys())
        import riskcloud.adapters.base  # noqa: F401
        after = set(sys.modules.keys())
        new_mods = after - before
        forbidden = [m for m in new_mods if any(
            m.startswith(p) for p in ("pandas", "numpy", "sklearn", "torch", "tensorflow")
        )]
        assert not forbidden, f"Adapter pulled in heavy dependencies: {forbidden}"


# -----------------------------------------------------------------
# 2. Documentation standards
# -----------------------------------------------------------------

class TestDocumentation:

    def test_contracts_have_docstrings(self):
        """Every public contract class must have a docstring."""
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
        """Each contract module must reference its Section in docs/design.md."""
        files = [
            ("riskcloud/contracts/event.py", "Section 6.1"),
            ("riskcloud/contracts/prediction_point.py", "Section 6.2"),
            ("riskcloud/contracts/feature_catalog.py", "Section 6.3"),
            ("riskcloud/contracts/document.py", "Section 6.4"),
        ]
        for rel_path, section in files:
            content = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
            assert section in content, (
                f"{rel_path} should mention {section}"
            )


# -----------------------------------------------------------------
# 3. Directory structure
# -----------------------------------------------------------------

class TestDirectoryStructure:

    def test_riskcloud_package_exists(self):
        assert (REPO_ROOT / "riskcloud").is_dir()

    def test_contracts_package_exists(self):
        assert (REPO_ROOT / "riskcloud" / "contracts").is_dir()

    def test_adapters_package_exists(self):
        assert (REPO_ROOT / "riskcloud" / "adapters").is_dir()

    def test_tests_directory_exists(self):
        assert (REPO_ROOT / "tests").is_dir()

    def test_docs_directory_exists(self):
        assert (REPO_ROOT / "docs").is_dir()

    def test_design_doc_exists(self):
        assert (REPO_ROOT / "docs" / "design.md").is_file()

    def test_readme_exists(self):
        assert (REPO_ROOT / "README.md").is_file()
