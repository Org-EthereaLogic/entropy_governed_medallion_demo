.PHONY: help install dev test lint demo visuals clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install the package in editable mode
	python -m pip install --upgrade pip
	python -m pip install -e .

dev: ## Install with dev + docs dependencies
	python -m pip install --upgrade pip
	python -m pip install -e ".[dev,docs]"

test: ## Run all tests with verbose output
	python -m pytest tests/ -v

lint: ## Run ruff linter on source and tests
	python -m ruff check src/ tests/

demo: ## Run the local entropy pipeline demo
	python -m entropy_governed_medallion.runners

visuals: ## Regenerate documentation visualizations
	python docs/generate_visuals.py

clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
