"""
Local demo runner for the entropy-governed medallion pipeline.

Processes sample CSVs through the full pipeline without Spark:
  1. Load baseline and current data from CSV
  2. Compute per-column Shannon Entropy (pure Python)
  3. Detect distribution drift
  4. Evaluate frozen KPI gates
  5. Build provenance envelope
  6. Print a human-readable report

Usage:
    python -m entropy_governed_medallion.runners.local_demo

Author: Anthony Johnson | EthereaLogic LLC
"""

from __future__ import annotations

import csv
import math
import subprocess
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from entropy_governed_medallion.config import load_gate_config
from entropy_governed_medallion.contracts import (
    FidelityResult,
    RunContext,
    RunStatus,
    TableHealthResult,
)
from entropy_governed_medallion.entropy.drift_detector import DriftDetector
from entropy_governed_medallion.gates.evaluator import evaluate_gates
from entropy_governed_medallion.provenance.builder import build_provenance_envelope

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
CONFIG_PATH = PROJECT_ROOT / "config" / "kpi_thresholds.json"

EXCLUDE_COLUMNS = {"employee_id", "first_name", "last_name"}


# --- Pure-Python entropy (mirrors shannon.py without PySpark) ---


def _column_entropy(values: List[str]) -> float:
    """Compute Shannon Entropy H(X) for a list of values."""
    n = len(values)
    if n == 0:
        return 0.0
    counts = Counter(values)
    entropy = 0.0
    for count in counts.values():
        p = count / n
        if p > 0:
            entropy -= p * math.log2(p)
    return round(entropy, 6)


def _normalized_entropy(values: List[str]) -> float:
    """Compute normalized Shannon Entropy H(X) / log2(n_distinct)."""
    h = _column_entropy(values)
    n_distinct = len(set(values))
    if n_distinct <= 1:
        return 0.0
    h_max = math.log2(n_distinct)
    return round(h / h_max, 6) if h_max > 0 else 0.0


def _classify_entropy(h_norm: float, n_distinct: int) -> str:
    if n_distinct <= 1 or h_norm == 0.0:
        return "CONSTANT"
    elif h_norm < 0.15:
        return "VERY_LOW"
    elif h_norm < 0.40:
        return "LOW"
    elif h_norm < 0.70:
        return "MODERATE"
    elif h_norm < 0.90:
        return "HIGH"
    else:
        return "VERY_HIGH"


# --- CSV loading ---


def load_csv(path: Path) -> List[Dict[str, str]]:
    """Load a CSV file into a list of row dicts."""
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def compute_entropy_profile(
    rows: List[Dict[str, str]],
    exclude: set[str] | None = None,
) -> List[Dict]:
    """Compute entropy profile for all non-excluded columns."""
    if not rows:
        return []
    exclude = exclude or set()
    columns = [c for c in rows[0].keys() if c not in exclude]
    total = len(rows)
    profile = []
    for col in columns:
        values = [r[col] for r in rows]
        h = _column_entropy(values)
        h_norm = _normalized_entropy(values)
        n_distinct = len(set(values))
        n_null = sum(1 for v in values if v == "" or v is None)
        profile.append({
            "column_name": col,
            "entropy": h,
            "normalized_entropy": h_norm,
            "distinct_count": n_distinct,
            "null_count": n_null,
            "null_ratio": round(n_null / total, 4) if total > 0 else 0.0,
            "total_count": total,
            "entropy_class": _classify_entropy(h_norm, n_distinct),
        })
    return profile


# --- Git helpers ---


def _git_info() -> tuple[str, str]:
    """Return (commit_hash, branch_name), falling back to unknowns."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        commit = "unknown"
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=PROJECT_ROOT, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        branch = "unknown"
    return commit, branch


# --- Report formatting ---


def _header(title: str) -> str:
    width = 60
    return f"\n{'=' * width}\n  {title}\n{'=' * width}"


def _print_profile(label: str, profile: List[Dict]) -> None:
    print(f"\n  {label}:")
    print(f"  {'Column':<16} {'Entropy':>8} {'Norm':>6} {'Distinct':>9} {'Class':<10}")
    print(f"  {'-' * 53}")
    for p in profile:
        print(
            f"  {p['column_name']:<16} "
            f"{p['entropy']:>8.4f} "
            f"{p['normalized_entropy']:>6.4f} "
            f"{p['distinct_count']:>9} "
            f"{p['entropy_class']:<10}"
        )


def _print_drift(health: TableHealthResult) -> None:
    print(f"\n  Health Score : {health.health_score:.4f}")
    print(f"  Gate Passed  : {health.passed_gate}")
    print(f"  Columns      : {health.total_columns_checked} checked, "
          f"{health.columns_drifted} drifted")
    if health.flagged_columns:
        print(f"\n  {'Column':<16} {'Direction':<10} {'Magnitude':>10} {'Stability':>10}")
        print(f"  {'-' * 50}")
        for c in health.flagged_columns:
            print(
                f"  {c.column_name:<16} "
                f"{c.drift_direction:<10} "
                f"{c.drift_magnitude:>10.4f} "
                f"{c.stability_score:>10.4f}"
            )


# --- Main pipeline ---


def run_demo(
    baseline_path: Path | None = None,
    current_path: Path | None = None,
) -> dict:
    """
    Run the full local entropy pipeline and return structured results.

    Returns a dict with keys: context, baseline_profile, current_profile,
    health, fidelity, gate_result, provenance.
    """
    baseline_csv = baseline_path or (SAMPLE_DIR / "employees_sample.csv")
    current_csv = current_path or (SAMPLE_DIR / "employees_drifted.csv")

    started = datetime.now(timezone.utc).isoformat()
    git_commit, git_branch = _git_info()
    run_id = f"local_{uuid.uuid4().hex[:8]}"

    # --- Phase 1: Load data ---
    baseline_rows = load_csv(baseline_csv)
    current_rows = load_csv(current_csv)

    # --- Phase 2: Compute entropy profiles ---
    baseline_profile = compute_entropy_profile(baseline_rows, EXCLUDE_COLUMNS)
    current_profile = compute_entropy_profile(current_rows, EXCLUDE_COLUMNS)

    # --- Phase 3: Drift detection ---
    gate_config_raw = load_gate_config(CONFIG_PATH)
    thresholds = {
        "entropy_drop_pct": 0.50,
        "entropy_spike_pct": 0.50,
        "health_score_floor": 0.70,
    }
    detector = DriftDetector(**thresholds)
    health = detector.compute_table_health(baseline_profile, current_profile)

    # --- Phase 4: Fidelity ---
    fidelity = FidelityResult(
        source_row_count=len(baseline_rows),
        target_row_count=len(current_rows),
        row_count_ratio=round(len(current_rows) / len(baseline_rows), 4)
        if baseline_rows else None,
        columns_match=set(baseline_rows[0].keys()) == set(current_rows[0].keys())
        if baseline_rows and current_rows else None,
    )

    # --- Phase 5: Gate evaluation ---
    context = RunContext(
        experiment_id="ENTROPY_MEDALLION_DEMO",
        run_id=run_id,
        git_commit=git_commit,
        git_branch=git_branch,
        operator="local_demo",
        started_at_utc=started,
        dry_run=False,
    )

    # Build a TableHealthResult contract for the gate evaluator
    health_contract = TableHealthResult(
        health_score=health.health_score,
        passed_gate=health.passed_gate,
        total_columns_checked=health.total_columns_checked,
        columns_drifted=health.columns_drifted,
        flagged_columns=health.flagged_columns,
        column_details=health.column_details,
    )

    completed = datetime.now(timezone.utc).isoformat()
    provenance = build_provenance_envelope(
        context=context,
        status=RunStatus.EXECUTION_COMPLETED,
        completed_at_utc=completed,
        catalog_name="local_demo",
        entropy_health=health_contract,
        tables_processed=("employees",),
    )

    gate_result = evaluate_gates(
        gate_config=gate_config_raw,
        context=context,
        entropy_health=health_contract,
        fidelity=fidelity,
        provenance=provenance,
        quality_pass_ratio=1.0,
        silver_quarantine_ratio=0.0,
    )

    return {
        "context": context,
        "baseline_profile": baseline_profile,
        "current_profile": current_profile,
        "health": health,
        "fidelity": fidelity,
        "gate_result": gate_result,
        "provenance": provenance,
    }


def main() -> None:
    """Run the demo and print a human-readable report."""
    results = run_demo()

    print(_header("ENTROPY-GOVERNED MEDALLION PIPELINE — LOCAL DEMO"))

    print(f"\n  Run ID   : {results['context'].run_id}")
    print(f"  Commit   : {results['context'].git_commit}")
    print(f"  Branch   : {results['context'].git_branch}")
    print(f"  Started  : {results['context'].started_at_utc}")

    # Profiles
    print(_header("PHASE 1 — ENTROPY PROFILES"))
    _print_profile("Baseline (employees_sample.csv)", results["baseline_profile"])
    _print_profile("Current  (employees_drifted.csv)", results["current_profile"])

    # Drift
    print(_header("PHASE 2 — DRIFT DETECTION"))
    _print_drift(results["health"])

    # Fidelity
    print(_header("PHASE 3 — FIDELITY CHECK"))
    fid = results["fidelity"]
    print(f"\n  Source rows  : {fid.source_row_count}")
    print(f"  Target rows  : {fid.target_row_count}")
    print(f"  Fidelity     : {fid.row_count_ratio}")
    print(f"  Schema match : {fid.columns_match}")

    # Gates
    print(_header("PHASE 4 — GATE EVALUATION"))
    gate = results["gate_result"]
    print(f"\n  Overall Verdict: {gate.overall_verdict}")
    print(f"\n  {'Gate':<34} {'Measured':>9} {'Op':>3} {'Thresh':>7} {'Type':<5} {'Result':<6}")
    print(f"  {'-' * 68}")
    for e in gate.evaluations:
        val = f"{e.measured_value:.4f}" if e.measured_value is not None else "N/A"
        passed = "PASS" if e.passed else ("FAIL" if e.passed is False else "N/A")
        print(
            f"  {e.metric:<34} {val:>9} {e.op:>3} {e.threshold:>7.2f} "
            f"{e.gate_type:<5} {passed:<6}"
        )

    # Provenance
    print(_header("PHASE 5 — PROVENANCE ENVELOPE"))
    prov = results["provenance"]
    print(f"\n  Experiment   : {prov.experiment_id}")
    print(f"  Run ID       : {prov.run_id}")
    print(f"  Git commit   : {prov.git_commit}")
    print(f"  Git branch   : {prov.git_branch}")
    print(f"  Catalog      : {prov.catalog_name}")
    print(f"  Operator     : {prov.operator}")
    print(f"  Started      : {prov.started_at_utc}")
    print(f"  Completed    : {prov.completed_at_utc}")
    print(f"  Health score : {prov.entropy_health_score}")
    print(f"  Cols drifted : {prov.columns_drifted}")
    print(f"  Verdict      : {prov.verdict}")

    verdict = gate.overall_verdict
    if verdict == "PASS":
        conclusion = "Gold refresh APPROVED — entropy distribution is stable."
    elif verdict == "WARN":
        conclusion = "Gold refresh approved with WARNINGS — review drifted columns."
    elif verdict == "FAIL":
        conclusion = "Gold refresh BLOCKED — entropy drift exceeds threshold."
    else:
        conclusion = "Gold refresh BLOCKED — incomplete gate measurements."

    print(_header("CONCLUSION"))
    print(f"\n  {conclusion}\n")


if __name__ == "__main__":
    main()
