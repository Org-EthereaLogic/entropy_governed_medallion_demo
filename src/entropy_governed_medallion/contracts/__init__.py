"""Typed contracts for the entropy-governed medallion pipeline.

Frozen dataclasses, explicit enums, and protocol-based gateway interfaces.
No mutable state in contracts.

Author: Anthony Johnson | EthereaLogic LLC
"""

from .models import (
    BronzeMetadata,
    CatalogConfig,
    CheckSeverity,
    ColumnDriftResult,
    DecisionRule,
    EntropyGateConfig,
    EntropyProfile,
    EntropyThresholds,
    ExecutionPlan,
    FailureBoundary,
    FidelityResult,
    GateDefinition,
    GateEvaluation,
    GateEvaluationResult,
    Guardrails,
    MaterializationOperation,
    MaterializationPlan,
    MaterializationResult,
    ProvenanceEnvelope,
    QualityRuleResult,
    RunContext,
    RunnerPhase,
    RunnerResult,
    RunStatus,
    SourceTableRef,
    TableHealthResult,
    TargetTableRef,
)

__all__ = [
    "BronzeMetadata",
    "CatalogConfig",
    "CheckSeverity",
    "ColumnDriftResult",
    "DecisionRule",
    "EntropyGateConfig",
    "EntropyProfile",
    "EntropyThresholds",
    "ExecutionPlan",
    "FailureBoundary",
    "FidelityResult",
    "GateDefinition",
    "GateEvaluation",
    "GateEvaluationResult",
    "Guardrails",
    "MaterializationOperation",
    "MaterializationPlan",
    "MaterializationResult",
    "ProvenanceEnvelope",
    "QualityRuleResult",
    "RunContext",
    "RunnerPhase",
    "RunnerResult",
    "RunStatus",
    "SourceTableRef",
    "TableHealthResult",
    "TargetTableRef",
]
