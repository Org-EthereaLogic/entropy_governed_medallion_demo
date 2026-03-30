# Claude Code Instructions (Entropy Governed Medallion Demo)

## Workspace Intent

This repository is a **public demonstration** of Shannon Entropy-based data
quality governance applied to Databricks medallion architecture. It is intended
to be published as a public GitHub repository.

## Non-Negotiable Rules

- **No proprietary algorithms.** This repository must never contain proprietary
  quality scoring methods or formulas. Shannon Entropy is the only governing
  algorithm permitted in this workspace.
- **No client data.** No client-specific data, names, or identifiers may appear
  in this repository.
- **No credentials.** No API keys, tokens, passwords, or connection strings.
- **Public-safe only.** Every file in this repo must be safe for public GitHub.

## Decision Order

1. `CLAUDE.md` (this file)
2. `config/kpi_thresholds.json`
3. `README.md`

## Architecture Pattern

Seam-based, typed-contract architecture with Shannon Entropy governance:

| Pattern | Description |
|---------|-------------|
| Shannon Entropy health scoring | Governing quality signal across all layers |
| Entropy health score gate | Gold refresh blocked when distribution drifts |
| Entropy + fidelity + quality gates | Multi-signal KPI threshold evaluation |
| Typed frozen contracts | Frozen dataclasses, explicit enums |
| Seam-based execution | Plan/execute separation per layer |
| Append-only evidence | Immutable evidence bundles per run |
| Provenance envelopes | Audit trail with entropy fields |

## Common Commands

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
pytest tests/ -v
ruff check src/ tests/
```
