.PHONY: help test test-verbose test-coverage lint format format-check install-hooks run-hooks clean dbt-test dbt-build dbt-docs

help:
	@echo "NFL Dead Money - Development Commands"
	@echo ""
	@echo "Testing:"
	@echo "  make test                 Run all tests"
	@echo "  make test-verbose         Run tests with verbose output"
	@echo "  make test-coverage        Run tests with coverage report (XML + HTML)"
	@echo "  make test-quick           Run tests excluding slow tests"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-test             Run dbt tests"
	@echo "  make dbt-build            Run dbt build (models + tests)"
	@echo "  make dbt-docs             Generate and serve dbt docs"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint                 Run linters (flake8, black, isort)"
	@echo "  make format               Format code (black, isort)"
	@echo "  make format-check         Check code formatting without changes"
	@echo ""
	@echo "Pre-commit:"
	@echo "  make install-hooks        Install pre-commit hooks"
	@echo "  make run-hooks            Run pre-commit hooks on all files"
	@echo ""
	@echo "Validation:"
	@echo "  make validate             Run dead money validator"
	@echo "  make validate-verbose     Run validator with detailed output"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean                Remove cache and build files"

test:
	./.venv/bin/pytest tests/ -q

test-verbose:
	./.venv/bin/pytest tests/ -v

test-coverage:
	./.venv/bin/pytest tests/ --cov=src --cov-report=html --cov-report=xml --cov-report=term-missing

test-quick:
	./.venv/bin/pytest tests/ -m "not slow" -q

lint:
	@echo "Running flake8..."
	./.venv/bin/flake8 src/ tests/ --max-line-length=120 --extend-ignore=E203,W503 || true
	@echo "Checking black formatting..."
	./.venv/bin/black --check src/ tests/ || true
	@echo "Checking import sorting..."
	./.venv/bin/isort --check-only src/ tests/ || true

format:
	./.venv/bin/black src/ tests/
	./.venv/bin/isort src/ tests/

format-check:
	./.venv/bin/black --check src/ tests/
	./.venv/bin/isort --check-only src/ tests/

install-hooks:
	./.venv/bin/pre-commit install

run-hooks:
	./.venv/bin/pre-commit run --all-files

validate:
	./.venv/bin/python src/dead_money_validator.py

validate-verbose:
	PYTHONPATH=. ./.venv/bin/python -u src/dead_money_validator.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache
	rm -rf src/__pycache__ tests/__pycache__
