# Entropy-Governed Medallion Pipeline Demo

### Shannon Entropy as a Data Quality Signal for Databricks Bronze/Silver/Gold

**Built by [Anthony Johnson](https://www.linkedin.com/in/anthonyjohnsonii/) | EthereaLogic LLC**

---

## What Makes This Different

Most medallion architecture demos stop at null checks and deduplication. This project introduces **Shannon Entropy as a governing data quality signal** across the entire pipeline — using information-theoretic measurement to detect distribution drift, schema instability, and quality degradation before they reach Gold.

The core idea: **if you can measure the information content of your data at each layer, you can build quality gates that catch problems rule-based checks miss.**

A column that passes every null check and type check can still be useless if its entropy has collapsed (e.g., 98% of values became the same constant after a source system change). Traditional validation wouldn't catch that. Entropy-based monitoring does.

---

## Architecture

```
Source Systems ──► Landing Zone ──► Bronze ──► Silver ──► Gold ──► Dashboards / AI
   (Raw Files)      (ADLS Gen2)     (Raw       (Clean     (KPI
                                     Delta)     Delta)     Tables)
                                       │          │          │
                                       └──── Entropy Monitor ─┘
                                             (Quality Signal)
```

**Data flow:** ADF moves raw extracts into ADLS Gen2. Databricks reads landed files into Bronze with source metadata. Silver enforces business rules, validates quality, and measures entropy baselines. Gold produces KPI-ready aggregates. At each transition, entropy scores are computed and compared against baselines to detect drift.

**Governance:** Unity Catalog provides centralized access control, lineage, and metadata across all layers.

---

## The Entropy Quality Framework

### Why Shannon Entropy?

Shannon Entropy (H) measures the information content of a data column's value distribution:

```
H(X) = -Σ p(xᵢ) × log₂(p(xᵢ))
```

| Entropy Level | What It Means | Data Quality Signal |
|--------------|---------------|-------------------|
| H = 0 | All values identical | Column is constant — possible upstream failure |
| H is low | Few dominant values | Low cardinality — check whether expected |
| H is moderate | Balanced distribution | Healthy variability |
| H is high | Many unique values | High cardinality — expected for IDs, timestamps |

### What This Framework Detects (That Traditional Checks Miss)

- **Silent source failures:** Column diversity collapses — source system defaulted
- **Schema drift:** New categories shift the distribution unexpectedly
- **Cardinality collapse:** Join keys that should be unique start clustering
- **Freshness decay:** Timestamp entropy drops because the same date repeats

### How It Works

1. **Baseline capture (Silver):** Entropy computed per column and stored as baseline
2. **Drift detection (each load):** New entropy compared to baseline; delta exceeding threshold triggers flag
3. **Composite health score:** Per-table health = weighted average of column stability scores (0–1)
4. **Gold gate:** Gold refresh blocked when health score falls below threshold (default: 0.70)

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Processing Engine | Azure Databricks (Spark) |
| Storage Format | Delta Lake |
| Languages | PySpark, SQL |
| Governance | Unity Catalog |
| Quality Framework | Shannon Entropy + rule-based validation |
| Gate Evaluation | Frozen KPI thresholds with typed contracts |
| Testing | pytest |
| Deployment | Databricks Asset Bundles |

---

## Project Structure

```
entropy_governed_medallion_demo/
├── README.md
├── LICENSE
├── pyproject.toml
├── config/
│   └── kpi_thresholds.json              # Frozen gate definitions
├── src/entropy_governed_medallion/
│   ├── contracts/
│   │   └── models.py                    # Frozen typed dataclasses
│   ├── entropy/
│   │   ├── shannon.py                   # Core Shannon Entropy computation
│   │   ├── drift_detector.py            # Drift detection + table health scoring
│   │   └── baseline.py                  # Baseline capture and retrieval
│   ├── seams/
│   │   ├── materialization.py           # Bronze CREATE TABLE AS SELECT
│   │   ├── fidelity.py                  # Source vs target row count ratio
│   │   ├── entropy_capture.py           # THE KEY SEAM — entropy governance
│   │   └── quality_rules.py             # Rule-based validation engine
│   ├── gates/
│   │   └── evaluator.py                 # Gate evaluator with entropy FAIL gate
│   ├── provenance/
│   │   └── builder.py                   # Append-only provenance envelopes
│   ├── runners/                         # State-machine execution runner
│   ├── evidence/                        # Append-only evidence bundles
│   └── config/                          # Configuration loading
├── notebooks/
│   └── 04_entropy_deep_dive.py          # Interactive Databricks demonstration
├── data/sample/
│   ├── employees_sample.csv             # Baseline enterprise data
│   └── employees_drifted.csv            # Same schema, collapsed distributions
├── tests/
│   ├── test_drift_detection.py          # Drift scenario tests
│   └── test_gate_evaluator.py           # Gate evaluation tests
├── docs/
│   └── architecture/                    # Architecture documentation
└── runs/                                # Append-only evidence bundles
```

---

## Quick Start

### 1. Clone and Install
```bash
git clone https://github.com/Org-EthereaLogic/entropy_governed_medallion_demo.git
cd entropy-governed-medallion-demo
pip install -e ".[dev]"
```

### 2. Run Tests Locally
```bash
pytest tests/ -v
```

### 3. Run the Entropy Deep Dive in Databricks
Upload `notebooks/04_entropy_deep_dive.py` to your Databricks workspace and run all cells. Uses `samples.nyctaxi.trips` — no uploads needed.

### 4. Explore Drift Detection
Compare `data/sample/employees_sample.csv` (healthy distribution) against `data/sample/employees_drifted.csv` (collapsed distributions). The entropy framework detects what null checks cannot.

---

## Gate Definitions

| Gate | Type | Threshold | What It Protects Against |
|------|------|-----------|------------------------|
| `entropy_health_score` | FAIL | >= 0.70 | Distribution drift reaching Gold |
| `bronze_record_fidelity_ratio` | FAIL | >= 0.99 | Row loss during ingestion |
| `silver_quality_pass_ratio` | FAIL | >= 0.95 | Data quality rule failures |
| `provenance_field_coverage` | FAIL | >= 1.0 | Missing audit trail fields |
| `entropy_columns_drifted_ratio` | WARN | <= 0.20 | Widespread distribution instability |
| `silver_quarantine_ratio` | WARN | <= 0.10 | Excessive quarantined records |

---

## What This Repo Does NOT Include

- Proprietary client data, formulas, or algorithms
- Production credentials or connection strings
- Enterprise networking or security configurations
- Proprietary quality scoring methods or formulas

---

## Author

**Anthony Johnson** — US-Based Databricks & Enterprise AI Solutions Architect

- LinkedIn: [linkedin.com/in/anthonyjohnsonii](https://www.linkedin.com/in/anthonyjohnsonii/)
- Company: EthereaLogic LLC

---

## License

MIT License. See [LICENSE](LICENSE) for details.
