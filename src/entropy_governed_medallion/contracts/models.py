"""
Frozen typed contracts for the entropy-governed medallion pipeline.

Every contract is a frozen dataclass. No mutable state.
Shannon Entropy governance primitives drive all quality signals.

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

# --- Enums ---

class RunStatus(Enum):
    PENDING = "PENDING"
    DRY_RUN_COMPLETED = "DRY_RUN_COMPLETED"
    EXECUTION_COMPLETED = "EXECUTION_COMPLETED"
    FAILED_CONFIG = "FAILED_CONFIG"
    FAILED_PREFLIGHT = "FAILED_PREFLIGHT"
    FAILED_PLANNING = "FAILED_PLANNING"
    FAILED_EXECUTION = "FAILED_EXECUTION"
    FAILED_EVIDENCE = "FAILED_EVIDENCE"


class RunnerPhase(Enum):
    LOAD_CONFIG = "LOAD_CONFIG"
    VALIDATE = "VALIDATE"
    INIT_BUNDLE = "INIT_BUNDLE"
    PREFLIGHT = "PREFLIGHT"
    PLAN = "PLAN"
    EXECUTE_BRONZE = "EXECUTE_BRONZE"
    EXECUTE_SILVER = "EXECUTE_SILVER"
    ENTROPY_BASELINE = "ENTROPY_BASELINE"
    ENTROPY_MEASURE = "ENTROPY_MEASURE"
    DRIFT_DETECT = "DRIFT_DETECT"
    EXECUTE_GOLD = "EXECUTE_GOLD"
    FIDELITY_CAPTURE = "FIDELITY_CAPTURE"
    RECORD_EVIDENCE = "RECORD_EVIDENCE"
    GATE_EVALUATION = "GATE_EVALUATION"
    SUMMARIZE = "SUMMARIZE"


class FailureBoundary(Enum):
    CONFIG = "CONFIG"
    PREFLIGHT = "PREFLIGHT"
    PLANNING = "PLANNING"
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    ENTROPY = "ENTROPY"
    GOLD = "GOLD"
    EVIDENCE = "EVIDENCE"


class CheckSeverity(Enum):
    FAIL = "FAIL"
    WARN = "WARN"
    INFO = "INFO"


# --- Table references ---

@dataclass(frozen=True)
class SourceTableRef:
    catalog: str
    schema: str
    table: str

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class TargetTableRef:
    catalog: str
    schema: str
    table: str

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


# --- Bronze metadata ---

@dataclass(frozen=True)
class BronzeMetadata:
    """The three required Bronze metadata columns for medallion ingestion audit trail."""
    source_system: str
    source_file_path: str
    ingest_ts: str  # ISO format timestamp


# --- Catalog config ---

@dataclass(frozen=True)
class CatalogConfig:
    catalog: str
    bronze_schemas: tuple[str, ...]
    silver_schemas: tuple[str, ...]
    gold_schemas: tuple[str, ...]
    quality_schema: str = "silver_quality"


# --- Entropy profile ---

@dataclass(frozen=True)
class EntropyProfile:
    """Entropy measurement for a single column at a point in time."""
    table_name: str
    column_name: str
    entropy: float
    normalized_entropy: float
    distinct_count: int
    null_count: int
    null_ratio: float
    total_count: int
    entropy_class: str
    measurement_ts: Optional[str] = None


# --- Drift results ---

@dataclass(frozen=True)
class ColumnDriftResult:
    column_name: str
    baseline_entropy: float
    current_entropy: float
    drift_detected: bool
    drift_direction: str  # STABLE | COLLAPSE | SPIKE
    drift_magnitude: float
    stability_score: float  # 0.0 = maximum drift, 1.0 = no drift


@dataclass(frozen=True)
class TableHealthResult:
    health_score: float
    passed_gate: bool
    total_columns_checked: int
    columns_drifted: int
    flagged_columns: tuple[ColumnDriftResult, ...]
    column_details: tuple[ColumnDriftResult, ...]


# --- Materialization ---

@dataclass(frozen=True)
class MaterializationOperation:
    name: str
    sql: str
    safe_to_repeat: bool = True


@dataclass(frozen=True)
class MaterializationPlan:
    source: SourceTableRef
    target: TargetTableRef
    write_disposition: str
    operations: tuple[MaterializationOperation, ...]


@dataclass(frozen=True)
class MaterializationResult:
    plan: MaterializationPlan
    executed: bool
    target_exists_before: bool
    target_exists_after: bool
    statements_attempted: int
    statements_executed: int


# --- Fidelity ---

@dataclass(frozen=True)
class FidelityResult:
    source_row_count: Optional[int]
    target_row_count: Optional[int]
    row_count_ratio: Optional[float]
    columns_match: Optional[bool]
    mismatched_columns: tuple[str, ...] = ()


# --- Quality rules ---

@dataclass(frozen=True)
class QualityRuleResult:
    rule_name: str
    column_name: str
    passed: bool
    records_checked: int
    records_failed: int
    failure_ratio: float
    action_taken: str  # QUARANTINE | REJECT | LOG


# --- Gate evaluation (entropy-governed) ---

@dataclass(frozen=True)
class GateDefinition:
    metric: str
    type: str  # FAIL | WARN
    op: str  # >= | <= | > | < | ==
    threshold: float
    reason: str


@dataclass(frozen=True)
class EntropyGateConfig:
    gates: tuple[GateDefinition, ...]
    guardrails: dict = field(default_factory=dict)
    decision_rule: dict = field(default_factory=dict)


@dataclass(frozen=True)
class GateEvaluation:
    gate_id: str
    metric: str
    measured_value: Optional[float]
    threshold: float
    op: str
    gate_type: str
    passed: Optional[bool]
    details: str


@dataclass(frozen=True)
class GateEvaluationResult:
    run_id: str
    evaluations: tuple[GateEvaluation, ...]
    overall_verdict: str  # PASS | WARN | FAIL | INCOMPLETE
    unmeasured_gates: tuple[str, ...]


# --- Execution plan ---

@dataclass(frozen=True)
class ExecutionPlan:
    run_id: str
    dry_run: bool
    phases: tuple[str, ...]


# --- Provenance ---

@dataclass(frozen=True)
class ProvenanceEnvelope:
    """Append-only provenance record for each pipeline execution."""
    experiment_id: str
    run_id: str
    git_commit: str
    git_branch: str
    catalog_name: str
    operator: str
    started_at_utc: str
    completed_at_utc: Optional[str] = None
    entropy_health_score: Optional[float] = None
    tables_processed: tuple[str, ...] = ()
    columns_drifted: int = 0
    verdict: Optional[str] = None
    cost_estimate_usd: Optional[float] = None


# --- Run context ---

@dataclass(frozen=True)
class RunContext:
    experiment_id: str
    run_id: str
    git_commit: str
    git_branch: str
    operator: str
    started_at_utc: str
    dry_run: bool
    phase_history: tuple[RunnerPhase, ...] = ()

    def advance(self, phase: RunnerPhase) -> RunContext:
        return RunContext(
            experiment_id=self.experiment_id,
            run_id=self.run_id,
            git_commit=self.git_commit,
            git_branch=self.git_branch,
            operator=self.operator,
            started_at_utc=self.started_at_utc,
            dry_run=self.dry_run,
            phase_history=self.phase_history + (phase,),
        )


# --- Runner result ---

@dataclass(frozen=True)
class RunnerResult:
    status: RunStatus
    context: RunContext
    provenance: Optional[ProvenanceEnvelope] = None
    gate_evaluation: Optional[GateEvaluationResult] = None
    entropy_health: Optional[TableHealthResult] = None
    fidelity: Optional[FidelityResult] = None
    failure_boundary: Optional[FailureBoundary] = None
