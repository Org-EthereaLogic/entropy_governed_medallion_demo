"""
Provenance envelope builder.

Creates an immutable, append-only provenance record for each pipeline
execution with entropy-specific fields: health_score and columns_drifted.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from entropy_governed_medallion.contracts import (
    ProvenanceEnvelope,
    RunContext,
    RunStatus,
    TableHealthResult,
)


def build_provenance_envelope(
    *,
    context: RunContext,
    status: RunStatus,
    completed_at_utc: str,
    catalog_name: str,
    entropy_health: TableHealthResult | None = None,
    tables_processed: tuple[str, ...] = (),
    cost_estimate_usd: float | None = None,
) -> ProvenanceEnvelope:
    """Build a frozen provenance envelope from execution results."""

    return ProvenanceEnvelope(
        experiment_id=context.experiment_id,
        run_id=context.run_id,
        git_commit=context.git_commit,
        git_branch=context.git_branch,
        catalog_name=catalog_name,
        operator=context.operator,
        started_at_utc=context.started_at_utc,
        completed_at_utc=completed_at_utc,
        entropy_health_score=entropy_health.health_score if entropy_health else None,
        tables_processed=tables_processed,
        columns_drifted=entropy_health.columns_drifted if entropy_health else 0,
        verdict=status.value,
        cost_estimate_usd=cost_estimate_usd,
    )
