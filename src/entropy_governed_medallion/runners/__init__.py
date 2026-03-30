"""Runner package for the entropy-governed medallion pipeline."""

from __future__ import annotations

__all__ = ["compute_entropy_profile", "load_csv", "run_demo"]


def __getattr__(name: str):
    """Load local demo helpers lazily to avoid module re-import warnings."""
    if name in __all__:
        from . import local_demo

        return getattr(local_demo, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
