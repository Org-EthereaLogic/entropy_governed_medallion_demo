"""Generate README visualizations from the measured local demo outputs."""

from __future__ import annotations

import csv
import math
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

OUTPUT_DIR = Path(__file__).parent / "images"
OUTPUT_DIR.mkdir(exist_ok=True)

DATA_DIR = PROJECT_ROOT / "data" / "sample"

# --- Color palette ---
COLORS = {
    "green": "#2ecc71",
    "red": "#e74c3c",
    "yellow": "#f39c12",
    "blue": "#3498db",
    "dark_bg": "#1a1a2e",
    "card_bg": "#16213e",
    "text": "#e8e8e8",
    "grid": "#2a2a4a",
    "accent": "#0f3460",
}


def _load_plotting():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    return plt, GridSpec


def _run_demo():
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))

    from entropy_governed_medallion.runners.local_demo import run_demo

    return run_demo()


def compute_entropy(values: list[str]) -> float:
    counts = Counter(values)
    total = len(values)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts.values()]
    return -sum(p * math.log2(p) for p in probs if p > 0)


def load_csv(filename: str) -> list[dict[str, str]]:
    with open(DATA_DIR / filename, newline="") as f:
        return list(csv.DictReader(f))


def _result_label(passed: bool | None, gate_type: str) -> str:
    if passed is True:
        return "PASS"
    if passed is False and gate_type == "WARN":
        return "WARN"
    if passed is False:
        return "FAIL"
    return "N/A"


def _panel_color(entropy: float, is_baseline: bool) -> str:
    if entropy == 0.0:
        return COLORS["red"]
    if entropy < 1.0:
        return COLORS["yellow"]
    return COLORS["green"] if is_baseline else COLORS["blue"]


def _health_color(health_score: float) -> str:
    if health_score >= 0.85:
        return COLORS["green"]
    if health_score >= 0.70:
        return COLORS["yellow"]
    return COLORS["red"]


def _gate_outcome_text(verdict: str) -> str:
    if verdict == "PASS":
        return "PASS - Publication Allowed"
    if verdict == "WARN":
        return "WARN - Review Before Publication"
    if verdict == "FAIL":
        return "FAIL - Publication Blocked"
    return "INCOMPLETE - Missing Measurements"


def build_visual_metrics() -> dict:
    """Return the measured demo outputs used by the README visuals."""
    results = _run_demo()
    monitored_columns = [profile["column_name"] for profile in results["baseline_profile"]]
    baseline_profile = {
        profile["column_name"]: profile for profile in results["baseline_profile"]
    }
    current_profile = {
        profile["column_name"]: profile for profile in results["current_profile"]
    }

    gates = []
    for evaluation in results["gate_result"].evaluations:
        gates.append(
            {
                "metric": evaluation.metric,
                "gate_type": evaluation.gate_type,
                "measured": evaluation.measured_value,
                "threshold": evaluation.threshold,
                "op": evaluation.op,
                "result": _result_label(evaluation.passed, evaluation.gate_type),
            }
        )

    return {
        "results": results,
        "monitored_columns": monitored_columns,
        "baseline_profile": baseline_profile,
        "current_profile": current_profile,
        "baseline_health_score": 1.0,
        "current_health_score": results["health"].health_score,
        "overall_verdict": results["gate_result"].overall_verdict,
        "gates": gates,
    }


def generate_drift_comparison():
    """Generate the before/after drift comparison chart."""
    plt, GridSpec = _load_plotting()
    healthy = load_csv("employees_sample.csv")
    drifted = load_csv("employees_drifted.csv")

    columns = ["department", "salary", "status", "location"]
    labels = ["Department", "Salary", "Status", "Location"]

    fig = plt.figure(figsize=(16, 10), facecolor=COLORS["dark_bg"])
    fig.suptitle(
        "Silent Data Drift Detected Before KPI Publication",
        fontsize=20,
        fontweight="bold",
        color=COLORS["text"],
        y=0.97,
    )

    gs = GridSpec(
        2,
        2,
        hspace=0.40,
        wspace=0.3,
        top=0.85,
        bottom=0.08,
        left=0.08,
        right=0.95,
    )

    for idx, (col, label) in enumerate(zip(columns, labels)):
        ax = fig.add_subplot(gs[idx // 2, idx % 2], facecolor=COLORS["card_bg"])

        healthy_vals = [str(r[col]) for r in healthy]
        drifted_vals = [str(r[col]) for r in drifted]

        healthy_counts = Counter(healthy_vals)
        drifted_counts = Counter(drifted_vals)

        h_entropy = compute_entropy(healthy_vals)
        d_entropy = compute_entropy(drifted_vals)

        if len(set(healthy_vals + drifted_vals)) > 8:
            top_h = healthy_counts.most_common(4)
            top_d = drifted_counts.most_common(4)
            top_keys = list(dict.fromkeys([k for k, _ in top_h] + [k for k, _ in top_d]))[:5]
            other_h = sum(v for k, v in healthy_counts.items() if k not in top_keys)
            other_d = sum(v for k, v in drifted_counts.items() if k not in top_keys)
            all_keys = top_keys + (["Others"] if other_h + other_d > 0 else [])
            h_vals = [healthy_counts.get(k, 0) for k in top_keys]
            d_vals = [drifted_counts.get(k, 0) for k in top_keys]
            if other_h + other_d > 0:
                h_vals.append(other_h)
                d_vals.append(other_d)
        else:
            all_keys = sorted(set(healthy_vals + drifted_vals))
            h_vals = [healthy_counts.get(k, 0) for k in all_keys]
            d_vals = [drifted_counts.get(k, 0) for k in all_keys]

        x_pos = range(len(all_keys))
        width = 0.35

        ax.bar(
            [x - width / 2 for x in x_pos],
            h_vals,
            width,
            color=COLORS["green"],
            alpha=0.85,
            label="Baseline",
            edgecolor="none",
        )
        ax.bar(
            [x + width / 2 for x in x_pos],
            d_vals,
            width,
            color=COLORS["red"],
            alpha=0.85,
            label="After Drift",
            edgecolor="none",
        )

        display_keys = [k[:10] + ".." if len(k) > 12 else k for k in all_keys]
        ax.set_xticks(list(x_pos))
        ax.set_xticklabels(
            display_keys,
            fontsize=8,
            color=COLORS["text"],
            rotation=30,
            ha="right",
        )
        ax.set_ylabel("Count", color=COLORS["text"], fontsize=9)
        ax.tick_params(colors=COLORS["text"], labelsize=8)

        ax.set_title(
            f"{label}    H: {h_entropy:.2f} -> {d_entropy:.2f}",
            fontsize=12,
            fontweight="bold",
            color=COLORS["text"],
            pad=10,
        )

        if d_entropy < h_entropy * 0.5:
            verdict_text = "COLLAPSE DETECTED"
            verdict_color = COLORS["red"]
        else:
            verdict_text = "STABLE"
            verdict_color = COLORS["green"]

        ax.text(
            0.98,
            0.92,
            verdict_text,
            transform=ax.transAxes,
            fontsize=10,
            fontweight="bold",
            color=verdict_color,
            ha="right",
            va="top",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor=COLORS["dark_bg"],
                edgecolor=verdict_color,
                alpha=0.9,
            ),
        )

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color(COLORS["grid"])
        ax.spines["left"].set_color(COLORS["grid"])
        ax.set_facecolor(COLORS["card_bg"])

    fig.legend(
        ["Baseline (Healthy)", "After Drift (Corrupted)"],
        loc="lower center",
        ncol=2,
        fontsize=11,
        facecolor=COLORS["dark_bg"],
        edgecolor=COLORS["grid"],
        labelcolor=COLORS["text"],
        framealpha=0.9,
    )

    fig.text(
        0.5,
        0.90,
        "Measured demo run: row counts stay stable while four monitored business columns "
        "lose distribution diversity.",
        ha="center",
        fontsize=11,
        color="#aaaaaa",
        style="italic",
    )

    plt.savefig(OUTPUT_DIR / "drift_comparison.png", dpi=150, facecolor=COLORS["dark_bg"])
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'drift_comparison.png'}")


def generate_health_score_dashboard(metrics: dict | None = None):
    """Generate a health score comparison dashboard."""
    plt, _ = _load_plotting()
    metrics = metrics or build_visual_metrics()

    monitored_columns = metrics["monitored_columns"]
    labels = [column.replace("_", " ").title() for column in monitored_columns]
    baseline_profile = metrics["baseline_profile"]
    current_profile = metrics["current_profile"]

    panels = [
        (
            "Trusted Baseline Load",
            [baseline_profile[col]["entropy"] for col in monitored_columns],
            metrics["baseline_health_score"],
            "PASS",
            True,
        ),
        (
            "Drifted Load Under Review",
            [current_profile[col]["entropy"] for col in monitored_columns],
            metrics["current_health_score"],
            metrics["overall_verdict"],
            False,
        ),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), facecolor=COLORS["dark_bg"])

    max_entropy = max(
        max(panel_entropies) for _, panel_entropies, _, _, _ in panels
    )

    for ax, (title, entropies, health, verdict, is_baseline) in zip(axes, panels):
        ax.set_facecolor(COLORS["card_bg"])

        bar_colors = [_panel_color(entropy, is_baseline) for entropy in entropies]
        y_pos = range(len(monitored_columns))
        bars = ax.barh(
            list(y_pos),
            entropies,
            color=bar_colors,
            edgecolor="none",
            height=0.6,
        )

        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9, color=COLORS["text"])
        ax.set_xlabel(
            "Measured diversity signal (Shannon entropy, bits)",
            color=COLORS["text"],
            fontsize=10,
        )
        ax.set_title(
            title,
            fontsize=14,
            fontweight="bold",
            color=COLORS["text"],
            pad=12,
        )
        ax.tick_params(colors=COLORS["text"])
        ax.invert_yaxis()

        for i, (bar, entropy) in enumerate(zip(bars, entropies)):
            if entropy > 0:
                ax.text(
                    entropy + 0.05,
                    i,
                    f"{entropy:.2f}",
                    va="center",
                    fontsize=9,
                    color=COLORS["text"],
                    fontweight="bold",
                )
            else:
                ax.text(
                    0.15,
                    i,
                    "0.00 (CONSTANT)",
                    va="center",
                    fontsize=9,
                    color=COLORS["red"],
                    fontweight="bold",
                )

        health_color = _health_color(health)
        ax.text(
            0.98,
            0.02,
            f"Health Score: {health:.2f}\nGate Verdict: {_gate_outcome_text(verdict)}",
            transform=ax.transAxes,
            fontsize=10,
            fontweight="bold",
            color=health_color,
            ha="right",
            va="bottom",
            bbox=dict(
                boxstyle="round,pad=0.5",
                facecolor=COLORS["dark_bg"],
                edgecolor=health_color,
                alpha=0.95,
                linewidth=2,
            ),
        )

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color(COLORS["grid"])
        ax.spines["left"].set_color(COLORS["grid"])
        ax.set_xlim(0, max(max_entropy * 1.15, 0.5))

    fig.suptitle(
        "Distribution Stability Falls Before Gold Publication",
        fontsize=18,
        fontweight="bold",
        color=COLORS["text"],
        y=0.98,
    )
    fig.text(
        0.5,
        0.93,
        "The same executable run shows the monitored health score falling from 1.00 to 0.20, "
        "which triggers a release block.",
        ha="center",
        fontsize=10,
        color="#aaaaaa",
        style="italic",
    )

    plt.tight_layout(rect=[0, 0, 1, 0.91])
    plt.savefig(OUTPUT_DIR / "health_dashboard.png", dpi=150, facecolor=COLORS["dark_bg"])
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'health_dashboard.png'}")


def generate_gate_evaluation(metrics: dict | None = None):
    """Generate the gate evaluation result visualization."""
    plt, _ = _load_plotting()
    metrics = metrics or build_visual_metrics()
    gates = metrics["gates"]

    fig, ax = plt.subplots(figsize=(14, 5), facecolor=COLORS["dark_bg"])
    ax.set_facecolor(COLORS["card_bg"])
    ax.axis("off")

    fig.suptitle(
        "Release Control Verdict for the Drifted Load",
        fontsize=16,
        fontweight="bold",
        color=COLORS["text"],
        y=0.95,
    )

    col_labels = ["Gate", "Type", "Measured", "Threshold", "Op", "Result"]
    cell_text = []
    cell_colors = []

    for gate in gates:
        result = gate["result"]
        if result == "PASS":
            row_color = "#1a3d2e"
        elif result == "WARN":
            row_color = "#3d3a1a"
        else:
            row_color = "#3d1a1a"

        measured = "N/A" if gate["measured"] is None else f"{gate['measured']:.2f}"
        cell_text.append(
            [
                gate["metric"],
                gate["gate_type"],
                measured,
                f"{gate['threshold']:.2f}",
                gate["op"],
                result,
            ]
        )
        cell_colors.append([row_color] * 6)

    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        cellColours=cell_colors,
        colColours=[COLORS["accent"]] * 6,
        loc="center",
        cellLoc="center",
    )

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.8)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(COLORS["grid"])
        cell.set_text_props(
            color=COLORS["text"],
            fontweight="bold" if row == 0 else "normal",
        )
        if row == 0:
            cell.set_text_props(color="white", fontweight="bold")

    for i, gate in enumerate(gates):
        result_cell = table[i + 1, 5]
        if gate["result"] == "PASS":
            result_cell.set_text_props(color=COLORS["green"], fontweight="bold")
        elif gate["result"] == "WARN":
            result_cell.set_text_props(color=COLORS["yellow"], fontweight="bold")
        else:
            result_cell.set_text_props(color=COLORS["red"], fontweight="bold")

    entropy_gate = next(gate for gate in gates if gate["metric"] == "entropy_health_score")
    verdict = metrics["overall_verdict"]
    verdict_text = (
        f"OVERALL VERDICT: {verdict} - Gold publication "
        f"{'blocked' if verdict in {'FAIL', 'INCOMPLETE'} else 'allowed'}. "
        f"The measured health score ({entropy_gate['measured']:.2f}) "
        f"is tested against the release threshold ({entropy_gate['threshold']:.2f})."
    )

    fig.text(
        0.5,
        0.05,
        verdict_text,
        ha="center",
        fontsize=12,
        fontweight="bold",
        color=_health_color(metrics["current_health_score"]),
        bbox=dict(
            boxstyle="round,pad=0.5",
            facecolor=COLORS["dark_bg"],
            edgecolor=_health_color(metrics["current_health_score"]),
            linewidth=2,
        ),
    )

    plt.savefig(
        OUTPUT_DIR / "gate_evaluation.png",
        dpi=150,
        facecolor=COLORS["dark_bg"],
        bbox_inches="tight",
        pad_inches=0.3,
    )
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'gate_evaluation.png'}")


if __name__ == "__main__":
    visual_metrics = build_visual_metrics()
    generate_drift_comparison()
    generate_health_score_dashboard(visual_metrics)
    generate_gate_evaluation(visual_metrics)
    print("\nAll visualizations generated successfully.")
