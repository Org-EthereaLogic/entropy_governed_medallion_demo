"""Generate README visualizations for the entropy-governed medallion demo."""

import csv
import math
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

OUTPUT_DIR = Path(__file__).parent / "images"
OUTPUT_DIR.mkdir(exist_ok=True)

DATA_DIR = Path(__file__).parent.parent / "data" / "sample"

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

BAR_COLORS_HEALTHY = ["#3498db", "#2ecc71", "#e67e22", "#9b59b6", "#1abc9c"]
BAR_COLORS_DRIFTED = ["#e74c3c"]


def compute_entropy(values: list[str]) -> float:
    counts = Counter(values)
    total = len(values)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts.values()]
    return -sum(p * math.log2(p) for p in probs if p > 0)


def load_csv(filename: str) -> list[dict]:
    with open(DATA_DIR / filename) as f:
        return list(csv.DictReader(f))


def generate_drift_comparison():
    """Generate the before/after drift comparison chart."""
    healthy = load_csv("employees_sample.csv")
    drifted = load_csv("employees_drifted.csv")

    columns = ["department", "salary", "status", "location"]
    labels = ["Department", "Salary", "Status", "Location"]

    fig = plt.figure(figsize=(16, 10), facecolor=COLORS["dark_bg"])
    fig.suptitle(
        "What Entropy Detects That Traditional Checks Miss",
        fontsize=20,
        fontweight="bold",
        color=COLORS["text"],
        y=0.97,
    )

    gs = GridSpec(2, 2, hspace=0.40, wspace=0.3, top=0.85, bottom=0.08, left=0.08, right=0.95)

    for idx, (col, label) in enumerate(zip(columns, labels)):
        ax = fig.add_subplot(gs[idx // 2, idx % 2], facecolor=COLORS["card_bg"])

        healthy_vals = [str(r[col]) for r in healthy]
        drifted_vals = [str(r[col]) for r in drifted]

        healthy_counts = Counter(healthy_vals)
        drifted_counts = Counter(drifted_vals)

        h_entropy = compute_entropy(healthy_vals)
        d_entropy = compute_entropy(drifted_vals)

        # For high-cardinality columns (like salary), bucket into top-5 + Others
        if len(set(healthy_vals + drifted_vals)) > 8:
            top_h = healthy_counts.most_common(4)
            top_d = drifted_counts.most_common(4)
            top_keys = list(dict.fromkeys(
                [k for k, _ in top_h] + [k for k, _ in top_d]
            ))[:5]
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
            [x - width / 2 for x in x_pos], h_vals, width,
            color=COLORS["green"], alpha=0.85, label="Baseline", edgecolor="none",
        )
        ax.bar(
            [x + width / 2 for x in x_pos], d_vals, width,
            color=COLORS["red"], alpha=0.85, label="After Drift", edgecolor="none",
        )

        # Truncate long labels
        display_keys = [k[:10] + ".." if len(k) > 12 else k for k in all_keys]
        ax.set_xticks(list(x_pos))
        ax.set_xticklabels(display_keys, fontsize=8, color=COLORS["text"], rotation=30, ha="right")
        ax.set_ylabel("Count", color=COLORS["text"], fontsize=9)
        ax.tick_params(colors=COLORS["text"], labelsize=8)

        ax.set_title(
            f"{label}    H: {h_entropy:.2f} \u2192 {d_entropy:.2f}",
            fontsize=12, fontweight="bold", color=COLORS["text"], pad=10,
        )

        # Add drift verdict
        if d_entropy < h_entropy * 0.5:
            verdict_text = "COLLAPSE DETECTED"
            verdict_color = COLORS["red"]
        else:
            verdict_text = "STABLE"
            verdict_color = COLORS["green"]

        ax.text(
            0.98, 0.92, verdict_text, transform=ax.transAxes,
            fontsize=10, fontweight="bold", color=verdict_color,
            ha="right", va="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor=COLORS["dark_bg"], edgecolor=verdict_color, alpha=0.9),
        )

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color(COLORS["grid"])
        ax.spines["left"].set_color(COLORS["grid"])
        ax.set_facecolor(COLORS["card_bg"])

    # Add legend at bottom
    fig.legend(
        ["Baseline (Healthy)", "After Drift (Corrupted)"],
        loc="lower center", ncol=2, fontsize=11,
        facecolor=COLORS["dark_bg"], edgecolor=COLORS["grid"],
        labelcolor=COLORS["text"],
        framealpha=0.9,
    )

    # Add subtitle
    fig.text(
        0.5, 0.90,
        "All rows pass null checks, type checks, and dedup checks in both datasets.  "
        "Only entropy measurement reveals the silent corruption.",
        ha="center", fontsize=11, color="#aaaaaa", style="italic",
    )

    plt.savefig(OUTPUT_DIR / "drift_comparison.png", dpi=150, facecolor=COLORS["dark_bg"])
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'drift_comparison.png'}")


def generate_health_score_dashboard():
    """Generate a health score comparison dashboard."""
    healthy = load_csv("employees_sample.csv")
    drifted = load_csv("employees_drifted.csv")

    columns = ["employee_id", "first_name", "last_name", "department",
               "hire_date", "status", "salary", "location"]
    labels = ["Employee ID", "First Name", "Last Name", "Department",
              "Hire Date", "Status", "Salary", "Location"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), facecolor=COLORS["dark_bg"])

    for ax, data, title, is_healthy in [
        (ax1, healthy, "Baseline (Week 1)", True),
        (ax2, drifted, "After Source Failure (Week 4)", False),
    ]:
        ax.set_facecolor(COLORS["card_bg"])

        entropies = []
        bar_colors = []
        for col in columns:
            h = compute_entropy([str(r[col]) for r in data])
            entropies.append(h)
            if h == 0:
                bar_colors.append(COLORS["red"])
            elif h < 1.0:
                bar_colors.append(COLORS["yellow"])
            else:
                bar_colors.append(COLORS["green"] if is_healthy else COLORS["blue"])

        y_pos = range(len(columns))
        bars = ax.barh(list(y_pos), entropies, color=bar_colors, edgecolor="none", height=0.6)

        ax.set_yticks(list(y_pos))
        ax.set_yticklabels(labels, fontsize=9, color=COLORS["text"])
        ax.set_xlabel("Shannon Entropy (bits)", color=COLORS["text"], fontsize=10)
        ax.set_title(title, fontsize=14, fontweight="bold", color=COLORS["text"], pad=12)
        ax.tick_params(colors=COLORS["text"])
        ax.invert_yaxis()

        # Add entropy values on bars
        for i, (bar, h) in enumerate(zip(bars, entropies)):
            if h > 0:
                ax.text(
                    h + 0.05, i, f"{h:.2f}", va="center",
                    fontsize=9, color=COLORS["text"], fontweight="bold",
                )
            else:
                ax.text(
                    0.15, i, "0.00 (CONSTANT)", va="center",
                    fontsize=9, color=COLORS["red"], fontweight="bold",
                )

        # Health score
        max_entropy = max(compute_entropy([str(r[col]) for r in healthy]) for col in columns)
        stability_scores = []
        baseline_entropies = [compute_entropy([str(r[col]) for r in healthy]) for col in columns]
        current_entropies = [compute_entropy([str(r[col]) for r in data]) for col in columns]
        for b, c in zip(baseline_entropies, current_entropies):
            if b == 0 and c == 0:
                stability_scores.append(1.0)
            elif b == 0:
                stability_scores.append(0.0)
            else:
                ratio = c / b
                stability_scores.append(min(ratio, 1.0))

        health = sum(stability_scores) / len(stability_scores)
        health_color = COLORS["green"] if health >= 0.85 else (COLORS["yellow"] if health >= 0.70 else COLORS["red"])
        verdict = "PASS" if health >= 0.70 else "FAIL"
        verdict_icon = "PASS - Gold Refresh Allowed" if health >= 0.70 else "FAIL - Gold Refresh Blocked"

        ax.text(
            0.98, 0.02,
            f"Health Score: {health:.2f}\nGate Verdict: {verdict_icon}",
            transform=ax.transAxes, fontsize=10, fontweight="bold",
            color=health_color, ha="right", va="bottom",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=COLORS["dark_bg"],
                      edgecolor=health_color, alpha=0.95, linewidth=2),
        )

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["bottom"].set_color(COLORS["grid"])
        ax.spines["left"].set_color(COLORS["grid"])
        ax.set_xlim(0, max(max(entropies) * 1.15, 0.5))

    fig.suptitle(
        "Entropy Health Dashboard: Per-Column Information Content",
        fontsize=18, fontweight="bold", color=COLORS["text"], y=0.98,
    )
    fig.text(
        0.5, 0.93,
        "Each bar shows the Shannon Entropy (information diversity) of a column.  "
        "Red = collapsed to a single value.  Green = healthy distribution.",
        ha="center", fontsize=10, color="#aaaaaa", style="italic",
    )

    plt.tight_layout(rect=[0, 0, 1, 0.91])
    plt.savefig(OUTPUT_DIR / "health_dashboard.png", dpi=150, facecolor=COLORS["dark_bg"])
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'health_dashboard.png'}")


def generate_gate_evaluation():
    """Generate the gate evaluation result visualization."""
    gates = [
        ("entropy_health_score", "FAIL", 0.18, 0.70, ">=", False),
        ("bronze_record_fidelity_ratio", "FAIL", 1.00, 0.99, ">=", True),
        ("silver_quality_pass_ratio", "FAIL", 1.00, 0.95, ">=", True),
        ("provenance_field_coverage", "FAIL", 1.00, 1.00, ">=", True),
        ("entropy_columns_drifted_ratio", "WARN", 0.50, 0.20, "<=", False),
        ("silver_quarantine_ratio", "WARN", 0.00, 0.10, "<=", True),
    ]

    fig, ax = plt.subplots(figsize=(14, 5), facecolor=COLORS["dark_bg"])
    ax.set_facecolor(COLORS["card_bg"])
    ax.axis("off")

    # Title
    fig.suptitle(
        "Gate Evaluation Matrix: Entropy-Drifted Dataset",
        fontsize=16, fontweight="bold", color=COLORS["text"], y=0.95,
    )

    # Table
    col_labels = ["Gate", "Type", "Measured", "Threshold", "Op", "Result"]
    cell_text = []
    cell_colors = []

    for name, gtype, measured, threshold, op, passed in gates:
        if passed:
            result = "PASS"
            row_color = "#1a3d2e"
        elif gtype == "WARN":
            result = "WARN"
            row_color = "#3d3a1a"
        else:
            result = "FAIL"
            row_color = "#3d1a1a"

        cell_text.append([
            name, gtype, f"{measured:.2f}", f"{threshold:.2f}", op, result,
        ])
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

    # Style header
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor(COLORS["grid"])
        cell.set_text_props(color=COLORS["text"], fontweight="bold" if row == 0 else "normal")
        if row == 0:
            cell.set_text_props(color="white", fontweight="bold")

    # Color the result cells
    for i, (_, _, _, _, _, passed) in enumerate(gates):
        result_cell = table[i + 1, 5]
        if passed:
            result_cell.set_text_props(color=COLORS["green"], fontweight="bold")
        elif gates[i][1] == "WARN":
            result_cell.set_text_props(color=COLORS["yellow"], fontweight="bold")
        else:
            result_cell.set_text_props(color=COLORS["red"], fontweight="bold")

    # Overall verdict
    fig.text(
        0.5, 0.05,
        "OVERALL VERDICT: FAIL \u2014 Gold table refresh blocked. Entropy health score (0.18) below threshold (0.70).",
        ha="center", fontsize=12, fontweight="bold", color=COLORS["red"],
        bbox=dict(boxstyle="round,pad=0.5", facecolor=COLORS["dark_bg"],
                  edgecolor=COLORS["red"], linewidth=2),
    )

    plt.savefig(OUTPUT_DIR / "gate_evaluation.png", dpi=150, facecolor=COLORS["dark_bg"],
                bbox_inches="tight", pad_inches=0.3)
    plt.close()
    print(f"Generated: {OUTPUT_DIR / 'gate_evaluation.png'}")


if __name__ == "__main__":
    generate_drift_comparison()
    generate_health_score_dashboard()
    generate_gate_evaluation()
    print("\nAll visualizations generated successfully.")
