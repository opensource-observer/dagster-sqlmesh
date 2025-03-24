# System detection
UNAME := "$(shell uname)"
PYTHON_VENV_NAME := ".venv"

# Python virtual environment settings
VENV_NAME := .venv
PYTHON := python

PYTHON_CMD := $(CURDIR)/$(VENV_NAME)/bin/python
SQLMESH_CMD := $(CURDIR)/$(VENV_NAME)/bin/sqlmesh
UV_CMD := uv
ACTIVATE := source $(CURDIR)/$(VENV_NAME)/bin/activate
DEACTIVATE := deactivate

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
clean-dagster:
	rm -rf sample/dagster_project/storage sample/dagster_project/logs sample/dagster_project/history

clean-db:
	$(PYTHON_CMD) -c "import duckdb; conn = duckdb.connect('db.db'); [conn.execute(cmd[0]) for cmd in conn.execute(\"\"\"SELECT 'DROP TABLE ' || table_schema || '.' || table_name || ' CASCADE;' as drop_cmd FROM information_schema.tables WHERE table_schema != 'sources' AND table_schema != 'information_schema' AND table_type = 'BASE TABLE'\"\"\").fetchall()]; [conn.execute(cmd[0]) for cmd in conn.execute(\"\"\"SELECT 'DROP VIEW ' || table_schema || '.' || table_name || ' CASCADE;' as drop_cmd FROM information_schema.tables WHERE table_schema != 'sources' AND table_schema != 'information_schema' AND table_type = 'VIEW'\"\"\").fetchall()]; conn.close()"

dagster-dev: clean-dagster
	@DAGSTER_HOME="$(subst \,/,$(CURDIR))/sample/dagster_project" "$(PYTHON_CMD)" -m dagster dev -f sample/dagster_project/definitions.py -h 0.0.0.0

dev: dagster-dev  # Alias for dagster-dev

dagster-materialize:
	$(PYTHON_CMD) -m dagster asset materialize -f sample/dagster_project/definitions.py --select '*'

sqlmesh-plan:
	cd sample/sqlmesh_project && $(SQLMESH_CMD) plan

sqlmesh-run:
	cd sample/sqlmesh_project && $(SQLMESH_CMD) run

clean-dev: clean-db clean-dagster dev

.PHONY: init init-python install-python-deps upgrade-python-deps clean test mypy check-pnpm install-node-deps upgrade-node-deps sample-dev dagster-dev dagster-materialize clean-dagster clean-db clean-dev 