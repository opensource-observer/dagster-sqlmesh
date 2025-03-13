# System detection
UNAME := "$(shell uname)"
PYTHON_VENV_NAME := ".venv"

# Python virtual environment settings
VENV_NAME := .venv
PYTHON := python

# Check if we're on Windows
ifeq ($(OS),Windows_NT)
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/Scripts/python
    SQLMESH_CMD := $(CURDIR)/$(VENV_NAME)/Scripts/sqlmesh
    UV_CMD := "$(subst \,/,$(USERPROFILE))/.local/bin/uv.exe"
    ACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/activate
    DEACTIVATE := source $(CURDIR)/$(VENV_NAME)/Scripts/deactivate
else
    PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/bin/python
    SQLMESH_CMD := $(CURDIR)/$(VENV_NAME)/bin/sqlmesh
    UV_CMD := uv
    ACTIVATE := source $(CURDIR)/$(VENV_NAME)/bin/activate
    DEACTIVATE := deactivate
endif


init-python:
	@if [ ! -d "$(PYTHON_VENV_NAME)" ]; then \
		echo "Creating virtual environment with Python 3.12..."; \
		uv venv --python 3.12 $(PYTHON_VENV_NAME); \
	fi

install-python-deps:
	uv sync --all-extras

upgrade-python-deps:
	uv lock --upgrade
	make install-python-deps

install-node-deps:
	pnpm install

upgrade-node-deps:
	pnpm update

# Base commands
init: init-python install-python-deps check-pnpm install-node-deps

clean:
	find . \( -type d -name "__pycache__" -o -type f -name "*.pyc" -o -type d -name ".pytest_cache" -o -type d -name "*.egg-info" \) -print0 | xargs -0 rm -rf

# Testing commands
test:
	$(PYTHON_CMD) -m pytest -vv --log-cli-level=INFO $(filter-out $@,$(MAKECMDGOALS))

pyright:
	pnpm pyright

# Sample project commands

dagster-dev: clean-dagster
	DAGSTER_HOME=$(CURDIR)/sample/dagster_project $(PYTHON_CMD) -m dagster dev -h 0.0.0.0 -w

dev: dagster-dev  # Alias for dagster-dev

dagster-materialize:
	$(PYTHON_CMD) -m dagster asset materialize -f sample/dagster_project/definitions.py --select '*'

.PHONY: init init-python install-python-deps upgrade-python-deps clean test mypy check-pnpm install-node-deps upgrade-node-deps sample-dev dagster-dev dagster-materialize clean-dagster 