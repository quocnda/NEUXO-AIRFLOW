set dotenv-load := true

# export COVERAGE_CORE := "sysmon"

export PYTHONDONTWRITEBYTECODE := "1"
export AIRFLOW__CORE__DAGS_FOLDER := justfile_directory() / "dags"

# Show this help message and exit
default:
    @just --list --unsorted

# -------------------
# Manage dependencies
# -------------------

install:
    uv sync --all-packages
    prek install --install-hooks


upgrade:
    uv lock --upgrade
    prek auto-update




format:
    uv run ruff check --fix .
    uv run ruff format .


lint:
    uv run ruff check .
    uv run ruff format --check .

typecheck:
    uv run mypy .

airflow:
    uv run airflow standalone

clean:
    rm -rf dist
    rm -rf .cache
    rm -rf .hypothesis
    rm -rf `find . -name __pycache__`
    rm -f `find . -type f -name '*.py[co]'`
