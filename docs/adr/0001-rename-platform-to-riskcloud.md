# ADR-0001: Rename top-level package from platform to riskcloud

**Status:** ACCEPTED
**Date:** 2026-07-23

## Context

The design document (`docs/design.md` Section 24) specified the top-level
package as `platform/`. However, Python's standard library has a module
named `platform` (`import platform` → `platform.python_version()` etc.).

Creating a directory named `platform/` at the repository root shadows the
stdlib module for Python interpreters running inside that directory, which
breaks pytest and other tools that call `platform.python_version()`.

## Decision

Rename `platform/` → `riskcloud/`.

The name mirrors the project name (LeakBench-RiskCloud) and has no
stdlib collision.

## Consequences

- All internal import paths use `riskcloud.contracts.*` instead of
  `platform.contracts.*`.
- The physical directory at the repo root is `riskcloud/`.
- This document serves as the canonical reference for the rename.
