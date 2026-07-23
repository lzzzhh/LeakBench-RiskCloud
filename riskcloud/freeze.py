"""Scientific freeze verification.

Verifies that the upstream LeakBench-Tab scientific core has not been
modified by checking the freeze lock against the live remote.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass(frozen=True)
class FreezeLock:
    upstream_repo: str
    upstream_commit: str
    protected_path: str
    protected_tree_sha: str

    @classmethod
    def load(cls, path: Path) -> "FreezeLock":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(
            upstream_repo=data["upstream"]["repository"],
            upstream_commit=data["upstream"]["commit"],
            protected_path=data["protected"]["path"],
            protected_tree_sha=data["protected"]["tree_sha"],
        )


@dataclass(frozen=True)
class FreezeResult:
    valid: bool
    lock: FreezeLock
    remote_commit: str = ""
    remote_tree_sha: str = ""
    errors: tuple[str, ...] = ()

    def report(self) -> str:
        lines = [
            f"  upstream repo:      {self.lock.upstream_repo}",
            f"  locked commit:      {self.lock.upstream_commit}",
            f"  remote HEAD:        {self.remote_commit or '(not checked)'}",
            f"  locked tree SHA:    {self.lock.protected_tree_sha}",
            f"  remote tree SHA:    {self.remote_tree_sha or '(not checked)'}",
            f"  protected path:     {self.lock.protected_path}",
            f"  status:             {'PASS' if self.valid else 'FAIL'}",
        ]
        if self.errors:
            lines.append("  errors:")
            for e in self.errors:
                lines.append(f"    - {e}")
        return "\n".join(lines)


def verify_freeze(lock_path: Path) -> FreezeResult:
    """Verify the freeze lock against the remote upstream repository."""
    if not lock_path.exists():
        return FreezeResult(
            valid=False,
            lock=FreezeLock("", "", "", ""),
            errors=("freeze lock file not found",),
        )

    lock = FreezeLock.load(lock_path)

    # Fetch remote commit
    try:
        result = subprocess.run(
            ["git", "ls-remote", f"https://github.com/{lock.upstream_repo}.git", "refs/heads/main"],
            capture_output=True, text=True, timeout=15,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        return FreezeResult(
            valid=False, lock=lock,
            errors=(f"failed to fetch remote: {exc}",),
        )

    if result.returncode != 0:
        return FreezeResult(
            valid=False, lock=lock,
            errors=(f"git ls-remote failed: {result.stderr.strip()}",),
        )

    parts = result.stdout.strip().split()
    remote_commit = parts[0] if parts else ""

    if remote_commit != lock.upstream_commit:
        return FreezeResult(
            valid=False,
            lock=lock,
            remote_commit=remote_commit,
            errors=(f"upstream commit changed: locked={lock.upstream_commit} remote={remote_commit}",),
        )

    # Fetch tree SHA for protected path
    try:
        result = subprocess.run(
            ["git", "ls-tree", lock.upstream_commit, lock.protected_path],
            capture_output=True, text=True, timeout=10,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        return FreezeResult(
            valid=False, lock=lock, remote_commit=remote_commit,
            errors=(f"failed to check tree: {exc}",),
        )

    # ls-tree output: <mode> tree <sha>\t<path>
    tree_sha = ""
    for line in result.stdout.strip().splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[1] == "tree":
            tree_sha = fields[2]
            break

    if tree_sha != lock.protected_tree_sha:
        return FreezeResult(
            valid=False,
            lock=lock,
            remote_commit=remote_commit,
            remote_tree_sha=tree_sha,
            errors=(f"protected tree changed: locked={lock.protected_tree_sha} remote={tree_sha}",),
        )

    return FreezeResult(
        valid=True,
        lock=lock,
        remote_commit=remote_commit,
        remote_tree_sha=tree_sha,
    )


# CLI entry point
if __name__ == "__main__":
    lock_path = Path(__file__).resolve().parents[2] / "scientific-freeze.lock"
    result = verify_freeze(lock_path)
    print(result.report())
    sys.exit(0 if result.valid else 1)
