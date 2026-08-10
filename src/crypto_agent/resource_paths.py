from __future__ import annotations

from pathlib import Path


_PACKAGE_DIRECTORY = Path(__file__).resolve().parent
_SOURCE_PROJECT_DIRECTORY = _PACKAGE_DIRECTORY.parents[1]


def default_risk_policy_path() -> Path:
    """Locate the immutable V1 policy in an installed wheel or source checkout."""

    return _first_existing(
        _PACKAGE_DIRECTORY / "resources" / "configs" / "risk_policy.v1.json",
        _SOURCE_PROJECT_DIRECTORY / "configs" / "risk_policy.v1.json",
        description="packaged V1 risk policy",
    )


def default_migration_directory() -> Path:
    """Locate checksummed migrations without depending on the process cwd."""

    return _first_existing(
        _PACKAGE_DIRECTORY / "resources" / "db" / "migrations",
        _SOURCE_PROJECT_DIRECTORY / "db" / "migrations",
        description="packaged PostgreSQL migrations",
    )


def _first_existing(*candidates: Path, description: str) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    rendered = ", ".join(str(candidate) for candidate in candidates)
    raise RuntimeError(f"Unable to locate {description}; checked: {rendered}")
