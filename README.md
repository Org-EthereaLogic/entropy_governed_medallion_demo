# Entropy-Governed Medallion Pipeline Demo

<p align="center">
  <a href="https://github.com/Org-EthereaLogic/entropy_governed_medallion_demo/actions/workflows/ci.yml"><img src="https://github.com/Org-EthereaLogic/entropy_governed_medallion_demo/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://app.codacy.com/gh/Org-EthereaLogic/entropy_governed_medallion_demo/dashboard"><img src="https://app.codacy.com/project/badge/Grade/09d6024a36ac43fb9e0e17b13d689d05" alt="Codacy Badge"></a>
  <a href="https://app.codacy.com/gh/Org-EthereaLogic/entropy_governed_medallion_demo/dashboard"><img src="https://app.codacy.com/project/badge/Coverage/09d6024a36ac43fb9e0e17b13d689d05" alt="Codacy Coverage"></a>
  <a href="https://codecov.io/gh/Org-EthereaLogic/entropy_governed_medallion_demo"><img src="https://codecov.io/gh/Org-EthereaLogic/entropy_governed_medallion_demo/graph/badge.svg" alt="codecov"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue" alt="Python 3.10 | 3.11 | 3.12"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License: MIT"></a>
</p>

**Shannon Entropy as a Data Quality Signal for Databricks Bronze/Silver/Gold**

**Built by [Anthony Johnson](https://www.linkedin.com/in/anthonyjohnsonii/) | EthereaLogic LLC**

---

<details>
<summary><strong>Table of Contents</strong></summary>

- [What Makes This Different](#what-makes-this-different)
- [Architecture](#architecture)
- [The Entropy Quality Framework](#the-entropy-quality-framework)
- [See It in Action](#see-it-in-action)
- [Gate Evaluation Matrix](#gate-evaluation-matrix)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Gate Definitions](#gate-definitions)
- [Contributing and Security](#contributing-and-security)

</details>

---

## What Makes This Different

Most medallion architecture demos stop at null checks and deduplication. This project introduces **Shannon Entropy as a governing data quality signal** across the entire pipeline — using information-theoretic measurement to detect distribution drift, schema instability, and quality degradation before they reach Gold.

**The core idea:** if you can measure the information content of your data at each layer, you can build quality gates that catch problems rule-based checks miss.

A column that passes every null check and type check can still be useless if its entropy has collapsed (e.g., 98% of values became the same constant after a source system change). Traditional validation wouldn't catch that. Entropy-based monitoring does.

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources ["Source Systems"]
        S1[Raw Files]
        S2[APIs]
    end

    subgraph Landing ["Landing Zone"]
        LZ[ADLS Gen2]
    end

    subgraph Medallion ["Databricks Medallion Pipeline"]
        direction LR
        B["Bronze\n(Raw Delta)"]
        SI["Silver\n(Clean Delta)"]
        G["Gold\n(KPI Tables)"]

        B -->|"Fidelity\nCheck"| SI
        SI -->|"Entropy\nGate"| G
    end

    subgraph Governance ["Entropy Governance"]
        EM["Entropy\nMonitor"]
        BL["Baseline\nStore"]
        GE["Gate\nEvaluator"]
        PE["Provenance\nEnvelope"]
    end

    subgraph Output ["Consumers"]
        D[Dashboards]
        AI[AI / ML]
    end

    S1 --> LZ
    S2 --> LZ
    LZ --> B
    G --> D
    G --> AI

    SI -.->|"measure"| EM
    EM -.->|"compare"| BL
    EM -.->|"evaluate"| GE
    GE -.->|"pass/fail"| G
    GE -.->|"record"| PE

    style B fill:#cd7f32,stroke:#8b5e3c,color:#fff
    style SI fill:#c0c0c0,stroke:#808080,color:#000
    style G fill:#ffd700,stroke:#b8960f,color:#000
    style EM fill:#e74c3c,stroke:#c0392b,color:#fff
    style GE fill:#e74c3c,stroke:#c0392b,color:#fff
    style BL fill:#3498db,stroke:#2980b9,color:#fff
    style PE fill:#2ecc71,stroke:#27ae60,color:#fff
```

**Data flow:** ADF moves raw extracts into ADLS Gen2. Databricks reads landed files into Bronze with source metadata. Silver enforces business rules, validates quality, and measures entropy baselines. Gold produces KPI-ready aggregates. At each transition, entropy scores are computed and compared against baselines to detect drift.

**Governance:** Unity Catalog provides centralized access control, lineage, and metadata across all layers.

---

## The Entropy Quality Framework

### Why Shannon Entropy?

Shannon Entropy (H) measures the **information diversity** of a data column's value distribution:

```text
H(X) = -Sigma p(xi) x log2(p(xi))
```

```mermaid
graph LR
    A["H = 0\nAll values identical"] --> B["H is low\nFew dominant values"]
    B --> C["H is moderate\nBalanced distribution"]
    C --> D["H is high\nMany unique values"]

    style A fill:#e74c3c,stroke:#c0392b,color:#fff
    style B fill:#f39c12,stroke:#d68910,color:#fff
    style C fill:#2ecc71,stroke:#27ae60,color:#fff
    style D fill:#3498db,stroke:#2980b9,color:#fff
```

| Entropy Level | What It Means | Data Quality Signal |
| ------------- | ------------- | ------------------- |
| H = 0 | All values identical | Column is constant -- possible upstream failure |
| H is low | Few dominant values | Low cardinality -- check whether expected |
| H is moderate | Balanced distribution | Healthy variability |
| H is high | Many unique values | High cardinality -- expected for IDs, timestamps |

### What This Framework Detects (That Traditional Checks Miss)

| Scenario | What Happened | Traditional Checks | Entropy Signal |
| -------- | ------------- | ------------------ | -------------- |
| Silent source failure | Column diversity collapses -- source system defaulted all values | All pass | **COLLAPSE DETECTED** |
| Schema drift | New categories shift the distribution unexpectedly | All pass | **SPIKE DETECTED** |
| Cardinality collapse | Join keys that should be unique start clustering | All pass | **COLLAPSE DETECTED** |
| Freshness decay | Timestamp entropy drops because the same date repeats | All pass | **COLLAPSE DETECTED** |

### How It Works

```mermaid
flowchart TD
    A["Silver Table Loaded"] --> B["Compute Shannon Entropy\nper column"]
    B --> C{"Baseline\nexists?"}
    C -->|"No (first load)"| D["Store as baseline\nin entropy_baselines"]
    C -->|"Yes"| E["Compare current vs\nbaseline entropy"]
    E --> F["Compute per-column\ndrift direction"]
    F --> G["Calculate composite\nhealth score (0-1)"]
    G --> H{"Health score\n>= 0.70?"}
    H -->|"Yes"| I["PASS\nGold refresh allowed"]
    H -->|"No"| J["FAIL\nGold refresh blocked"]
    I --> K["Record in\nprovenance envelope"]
    J --> K

    style A fill:#c0c0c0,stroke:#808080,color:#000
    style D fill:#3498db,stroke:#2980b9,color:#fff
    style I fill:#2ecc71,stroke:#27ae60,color:#fff
    style J fill:#e74c3c,stroke:#c0392b,color:#fff
    style K fill:#2ecc71,stroke:#27ae60,color:#fff
```

---

## See It in Action

The following visualizations use the included sample datasets (`data/sample/employees_sample.csv` and `data/sample/employees_drifted.csv`) to demonstrate how entropy governance catches silent data corruption.

### Drift Detection: Before vs After

Both datasets pass null checks, type checks, and deduplication. Only entropy measurement reveals that four columns collapsed to a single constant value.

<p align="center">
  <img src="docs/images/drift_comparison.png" alt="Drift comparison showing entropy collapse across department, salary, status, and location columns" width="900"/>
</p>

### Entropy Health Dashboard

A per-column view of information content. The baseline (Week 1) shows healthy distribution across all columns. After a simulated source failure (Week 4), four columns drop to zero entropy -- the health score falls from 1.00 to 0.50, triggering a gate failure.

<p align="center">
  <img src="docs/images/health_dashboard.png" alt="Entropy health dashboard comparing baseline and drifted column entropy" width="900"/>
</p>

### Gate Evaluation Matrix

The gate evaluator checks six quality thresholds before allowing a Gold table refresh. Even though five of six gates pass, the entropy health score failure blocks the entire pipeline -- preventing corrupted data from reaching executive dashboards.

<p align="center">
  <img src="docs/images/gate_evaluation.png" alt="Gate evaluation matrix showing entropy health score failure blocking Gold refresh" width="800"/>
</p>

---

## Technology Stack

| Component | Technology |
| --------- | ---------- |
| Processing Engine | Azure Databricks (Spark) |
| Storage Format | Delta Lake |
| Languages | PySpark, SQL |
| Governance | Unity Catalog |
| Quality Framework | Shannon Entropy + rule-based validation |
| Gate Evaluation | Frozen KPI thresholds with typed contracts |
| Testing | pytest |
| Deployment | Databricks Asset Bundles |
| Automation | GitHub Actions CI + GitHub release artifacts |

## Automation

- **CI:** GitHub Actions runs `pytest` and `ruff` across Python 3.10, 3.11, and 3.12 on pushes and pull requests.
- **Commit validation:** Commitizen enforces Conventional Commits in pull requests and direct pushes.
- **Coverage:** Push builds generate `coverage.xml` and upload to both Codacy and Codecov.
- **Release delivery:** Version tags and manual dispatches build wheel and source distributions, upload them as workflow artifacts, and publish GitHub release assets on tag pushes.

---

## Quick Start

### 1. Clone and Install

Use Python 3.10 or newer. The commands below use `python3.12`; replace it with `python3.10` or `python3.11` if that is the supported interpreter installed on your machine.

```bash
git clone https://github.com/Org-EthereaLogic/entropy_governed_medallion_demo.git
cd entropy_governed_medallion_demo
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

### 2. Run Tests Locally

```bash
pytest tests/ -v
```

### 3. Run the Entropy Deep Dive in Databricks

Upload `notebooks/04_entropy_deep_dive.py` to your Databricks workspace and run all cells. Uses `samples.nyctaxi.trips` -- no uploads needed.

### 4. Explore Drift Detection

Compare `data/sample/employees_sample.csv` (healthy distribution) against `data/sample/employees_drifted.csv` (collapsed distributions). The entropy framework detects what null checks cannot.

### 5. Regenerate README Visuals

The README charts are reproducible from the sample CSVs in this repository.

```bash
. .venv/bin/activate
python -m pip install -e ".[docs]"
python docs/generate_visuals.py
```

---

## Project Structure

```text
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
│   ├── images/                          # Generated visualizations
│   └── generate_visuals.py              # Visualization generation script
└── runs/                                # Append-only evidence bundles
```

---

## Gate Definitions

| Gate | Type | Threshold | What It Protects Against |
| ---- | ---- | --------- | ------------------------ |
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

## Contributing and Security

Contributions should preserve the repository's public-safe constraints, typed-contract architecture, and Shannon Entropy governance model. Follow the contribution workflow and conventional commit rules in [CONTRIBUTING.md](CONTRIBUTING.md).

If you discover a security issue, report it privately using the process in [SECURITY.md](SECURITY.md). Do not open public issues for sensitive disclosures.

---

## Author

**Anthony Johnson** -- US-Based Databricks & Enterprise AI Solutions Architect

- LinkedIn: [linkedin.com/in/anthonyjohnsonii](https://www.linkedin.com/in/anthonyjohnsonii/)
- Company: EthereaLogic LLC

---

## License

MIT License. See [LICENSE](LICENSE) for details.
