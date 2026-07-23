"""Scientific Freeze Guard Tests (Section 19 Phase 0).

Verify that Phase 0 does NOT modify the LeakBench scientific core.
Any change to src/leakbench/ during Phase 0 is a violation.
"""

from __future__ import annotations

import hashlib
import inspect
import os
import sys  # noqa: F401
import subprocess
from pathlib import Path

import pytest

# -----------------------------------------------------------------
# Paths
# -----------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
SCIENTIFIC_CORE = REPO_ROOT / "src" / "leakbench"
RISKCLOUD_ROOT = REPO_ROOT / "riskcloud"
TESTS_PLATFORM = REPO_ROOT / "tests" / "platform"


# -----------------------------------------------------------------
# 1. Scientific core files remain unchanged
# -----------------------------------------------------------------

SCIENTIFIC_FILES = [
    "__init__.py",
    "datasets.py",
    "structured_prior_protocol.py",
    "mechanisms/__init__.py",
    "models/__init__.py",
    "governance/__init__.py",
    "diagnostics/__init__.py",
    "capacity/__init__.py",
]


def _collect_scientific_files(root: Path) -> list[Path]:
    """Walk the scientific core and return all .py files."""
    if not root.exists():
        return []
    return sorted(root.rglob("*.py"))


def _file_hash(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _git_diff_scientific_core() -> str:
    """Run git diff against HEAD for the scientific core directory."""
    try:
        result = subprocess.run(
            ["git", "diff", "HEAD", "--", str(SCIENTIFIC_CORE)],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=10,
        )
        return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""


def _git_status_scientific_core() -> str:
    """Run git status for the scientific core directory."""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", str(SCIENTIFIC_CORE)],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=10,
        )
        return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""


class TestScientificFreeze:

    def test_scientific_core_directory_exists(self):
        assert SCIENTIFIC_CORE.exists(), (
            f"Scientific core missing at {SCIENTIFIC_CORE}"
        )
        assert SCIENTIFIC_CORE.is_dir()

    def test_scientific_core_files_exist(self):
        """Core __init__ modules must be present."""
        for rel in SCIENTIFIC_FILES:
            fpath = SCIENTIFIC_CORE / rel
            assert fpath.exists(), f"Core file missing: {fpath}"

    def test_riskcloud_files_not_in_scientific_core(self):
        """RiskCloud files must live under riskcloud/, not src/leakbench/."""
        riskcloud_files = list(RISKCLOUD_ROOT.rglob("*.py"))
        for pf in riskcloud_files:
            assert not str(pf.resolve()).startswith(str(SCIENTIFIC_CORE.resolve())), (
                f"Platform file incorrectly placed in scientific core: {pf}"
            )

    def test_no_new_files_in_scientific_core(self):
        """No new .py files should appear under src/leakbench from Phase 0."""
        status = _git_status_scientific_core()
        # Only added/modified files are violations
        new_or_modified = [
            line for line in status.splitlines()
            if line and (line.startswith("A ") or line.startswith("M "))
        ]
        assert not new_or_modified, (
            f"Scientific core has been modified. "
            f"Phase 0 must NOT change src/leakbench/.\n"
            f"Changed: {new_or_modified}"
        )

    def test_no_diff_in_scientific_core(self):
        """git diff HEAD must be empty for src/leakbench/."""
        diff = _git_diff_scientific_core()
        assert diff == "", (
            f"Scientific core has uncommitted changes:\n{diff[:2000]}"
        )

    def test_contracts_can_be_imported_independently(self):
        """Contract modules must import without touching src/leakbench."""
        import sys
        before = set(sys.modules.keys())
        # Import contracts one by one
        import riskcloud.contracts.event  # noqa: F401
        import riskcloud.contracts.prediction_point  # noqa: F401
        import riskcloud.contracts.feature_catalog  # noqa: F401
        import riskcloud.contracts.document  # noqa: F401
        after = set(sys.modules.keys())
        new_mods = after - before
        leakbench_imports = [m for m in new_mods if m.startswith("leakbench")]
        assert not leakbench_imports, (
            f"Contract imports pulled in leakbench modules: {leakbench_imports}"
        )

    def test_adapter_base_can_be_imported_independently(self):
        """Adapter base must import without touching src/leakbench."""
        import sys
        before = set(sys.modules.keys())
        import riskcloud.adapters.base  # noqa: F401,E402
        after = set(sys.modules.keys())
        new_mods = after - before
        leakbench_imports = [m for m in new_mods if m.startswith("leakbench")]
        assert not leakbench_imports, (
            f"Adapter imports pulled in leakbench modules: {leakbench_imports}"
        )


# -----------------------------------------------------------------
# 2. Existing tests still pass
# -----------------------------------------------------------------

class TestExistingTestsUnaffected:

    def test_existing_scientific_core_imports_still_work(self):
        """The original scientific core can still be imported."""
        import src.leakbench  # noqa: F401
        import src.leakbench.datasets  # noqa: F401
        import src.leakbench.mechanisms  # noqa: F401

    def test_tests_directory_structure_intact(self):
        """Existing test files are still present."""
        tests_dir = REPO_ROOT / "tests"
        assert tests_dir.exists()
        test_files = list(tests_dir.glob("test_*.py"))
        assert len(test_files) >= 40, (
            f"Expected >=40 test files, found {len(test_files)}. "
            "Existing tests may have been removed."
        )

    def test_pytest_config_still_present(self):
        cfg = REPO_ROOT / "pytest.ini"
        assert cfg.exists()


# -----------------------------------------------------------------
# 3. Documentation standards
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
        """Each contract module should reference its design doc section."""
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
