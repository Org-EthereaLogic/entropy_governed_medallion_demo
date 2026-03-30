# Contributing

## Scope

This repository is a public demonstration of Shannon Entropy-governed data quality patterns for Databricks medallion architecture. Every contribution must remain safe for public GitHub.

Do not introduce:

- Proprietary algorithms or scoring formulas
- Client data, names, or identifiers
- Secrets, credentials, or connection strings

## Local Setup

Use Python 3.10 or newer. The example below uses `python3.12`; replace it with `python3.10` or `python3.11` if that is the supported interpreter installed on your machine.

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Required Validation

Run these commands before opening a pull request:

```bash
pytest tests/ -v --cov=entropy_governed_medallion --cov-report=xml
ruff check src/ tests/
```

## Commit Convention

Use Conventional Commits for every commit:

```text
<type>(optional-scope): short summary
```

Accepted types for this repository include:

- `feat`
- `fix`
- `docs`
- `test`
- `build`
- `ci`
- `chore`
- `refactor`

Keep commit summaries imperative, focused, and concise. Prefer one logical change per commit.

## Pull Requests

Each pull request should:

- Explain the user-visible or architectural impact
- Include the validation commands you ran
- Keep the change set narrowly scoped
- Preserve the typed-contract and seam-based design patterns already in use
