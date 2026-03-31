# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Conventional Commits](https://www.conventionalcommits.org/).

## [Unreleased]

### Changed

- Reframed the public documentation for technology leaders, with an executive-first README, a dedicated technical appendix, and exhibit-style visuals tied to measured demo results.
- Aligned public metadata and support/contribution copy around the repository's core control pattern: blocking silent data drift before KPI publication while preserving auditability.

## [0.1.0] - 2026-03-30

### Added

- Core entropy governance library: Shannon Entropy computation, normalized entropy, and table profiling (`entropy/shannon.py`).
- Drift detection engine comparing current entropy against stored baselines (`entropy/drift_detector.py`, `entropy/baseline.py`).
- Frozen typed contracts for all pipeline primitives (`contracts/models.py`).
- Gate evaluator with `entropy_health_score` as a first-class FAIL gate (`gates/evaluator.py`).
- Seam-based execution modules: entropy capture, fidelity, materialization, quality rules (`seams/`).
- Provenance envelope builder with entropy-specific fields (`provenance/builder.py`).
- Configuration loader for frozen KPI gate thresholds (`config/`, `config/kpi_thresholds.json`).
- Local demo runner processing sample CSVs through the full pipeline without Spark (`runners/local_demo.py`).
- Bundled package resources for non-editable installs (`resources/`).
- Sample datasets: healthy baseline and drifted distributions (`data/sample/`).
- Databricks notebook for interactive entropy exploration (`notebooks/04_entropy_deep_dive.py`).
- Visualization generator for README charts sourced from measured demo outputs (`docs/generate_visuals.py`).
- Unit and integration tests covering drift detection, gate evaluation, packaged resources, and end-to-end demo execution.
- GitHub Actions CI with Python 3.10/3.11/3.12 matrix, Codecov, and Codacy integration.
- Conventional Commits validation workflow.
- Release workflow building and publishing wheel/sdist artifacts.
- Makefile with `install`, `dev`, `test`, `lint`, `demo`, `visuals`, and `clean` targets.
- README, technical appendix, visualizations, and Quick Start for the public demonstration.
- CONTRIBUTING, SECURITY, and LICENSE (MIT) documentation.
