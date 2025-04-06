import datetime as dt
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import typing as t
from dataclasses import dataclass, field

import duckdb
import polars
import pytest
from sqlmesh.core.config import (
    Config as SQLMeshConfig,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.console import get_console
from sqlmesh.utils.date import TimeLike

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.controller.base import PlanOptions, RunOptions
from dagster_sqlmesh.controller.dagster import DagsterSQLMeshController
from dagster_sqlmesh.events import ConsoleRecorder

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def setup_debug_logging_for_tests() -> None:
    root_logger = logging.getLogger(__name__.split(".")[0])
    root_logger.setLevel(logging.DEBUG)

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


@pytest.fixture
def sample_project_root() -> t.Iterator[str]:
    """Creates a temporary project directory containing both SQLMesh and Dagster projects"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        project_dir = shutil.copytree("sample", tmp_dir, dirs_exist_ok=True)

        yield project_dir

        # Create debug directory with timestamp AFTER test run
        debug_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "debug_runs"
        )
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_debug_dir = os.path.join(debug_dir, f"run_{timestamp}")

        # Copy contents to debug directory
        try:
            shutil.copytree(tmp_dir, run_debug_dir, dirs_exist_ok=True)
            logger.info(
                f"Copied final test project contents to {run_debug_dir} for debugging"
            )
        except FileNotFoundError:
            logger.warning(
                f"Temporary directory {tmp_dir} not found during cleanup copy."
            )
        except Exception as e:
            logger.error(
                f"Error copying temporary directory {tmp_dir} to {run_debug_dir}: {e}"
            )


@pytest.fixture
def sample_sqlmesh_project(sample_project_root: str) -> t.Iterator[str]:
    """Returns path to the SQLMesh project within the sample project"""
    sqlmesh_project_dir = os.path.join(sample_project_root, "sqlmesh_project")
    db_path = os.path.join(sqlmesh_project_dir, "db.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    yield sqlmesh_project_dir


@pytest.fixture
def sample_dagster_project(sample_project_root: str) -> t.Iterator[str]:
    """Returns path to the Dagster project within the sample project"""
    dagster_project_dir = os.path.join(sample_project_root, "dagster_project")
    sqlmesh_project_dir = os.path.join(sample_project_root, "sqlmesh_project")

    db_path = os.path.join(sqlmesh_project_dir, "db.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    yield dagster_project_dir


@dataclass
class SQLMeshTestContext:
    """A test context for running SQLMesh"""

    db_path: str
    context_config: SQLMeshContextConfig
    project_path: str

    # Internal state for backup/restore
    _backed_up_files: set[str] = field(default_factory=set, init=False)

    def create_controller(
        self, enable_debug_console: bool = False
    ) -> DagsterSQLMeshController:
        console = None
        if enable_debug_console:
            console = get_console()
        return DagsterSQLMeshController.setup_with_config(
            self.context_config, debug_console=console
        )

    def get_model_path(self, model_name: str) -> str:
        """Get the full path to a model file.

        Args:
            model_name: The name of the model file (e.g. 'staging_model_1.sql')

        Returns:
            str: Full path to the model file
        """
        # Common model directories to search
        model_dirs = [
            os.path.join(self.project_path, "models"),
            os.path.join(self.project_path, "models", "staging"),
            os.path.join(self.project_path, "models", "intermediate"),
            os.path.join(self.project_path, "models", "mart"),
        ]

        for directory in model_dirs:
            if not os.path.exists(directory):
                continue
            for root, _, files in os.walk(directory):
                if model_name in files:
                    return os.path.join(root, model_name)

        raise FileNotFoundError(f"Model file {model_name} not found in project")

    def backup_model_file(self, model_name: str) -> None:
        """Create a backup of a model file.

        Args:
            model_name: The name of the model file to backup
        """
        model_path = self.get_model_path(model_name)
        backup_path = f"{model_path}.bak"
        shutil.copy2(model_path, backup_path)

    def restore_model_file(self, model_name: str) -> None:
        """Restore a model file from its backup.

        Args:
            model_name: The name of the model file to restore
        """
        model_path = self.get_model_path(model_name)
        backup_path = f"{model_path}.bak"
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, model_path)
            os.remove(backup_path)

    def modify_model_file(self, model_name: str, new_content: str) -> None:
        """Modify a model file with new content, creating a backup first.

        Args:
            model_name: The name of the model file to modify
            new_content: The new content for the model file
        """
        model_path = self.get_model_path(model_name)
        if not hasattr(self, "_backed_up_files"):
            self._backed_up_files: set[str] = set()

        # Create backup if not already done
        if model_name not in self._backed_up_files:
            self.backup_model_file(model_name)
            self._backed_up_files.add(model_name)

        # Write new content
        with open(model_path, "w") as f:
            f.write(new_content)

    def cleanup_modified_files(self) -> None:
        """Restore all modified model files from their backups."""
        if hasattr(self, "_backed_up_files"):
            for model_name in self._backed_up_files:
                self.restore_model_file(model_name)
            self._backed_up_files.clear()

    def save_sqlmesh_debug_state(self, name_suffix: str = "manual_save") -> str:
        """Saves the current state of the SQLMesh project to the debug directory.

        Copies the contents of the SQLMesh project directory (self.project_path)
        to a timestamped sub-directory within the 'debug_runs' folder.

        Args:
            name_suffix: An optional suffix to append to the debug directory name
                         to distinguish this save point (e.g., 'before_change',
                         'after_plan'). Defaults to 'manual_save'.

        Returns:
            The path to the created debug state directory.
        """
        debug_dir_base = os.path.join(
            os.path.dirname(self.project_path), "..", "debug_runs"
        )
        os.makedirs(debug_dir_base, exist_ok=True)
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        run_debug_dir = os.path.join(
            debug_dir_base, f"sqlmesh_state_{timestamp}_{name_suffix}"
        )

        try:
            shutil.copytree(self.project_path, run_debug_dir, dirs_exist_ok=True)
            logger.info(f"Saved SQLMesh project debug state to {run_debug_dir}")
            return run_debug_dir
        except Exception as e:
            logger.error(
                f"Error saving SQLMesh project debug state to {run_debug_dir}: {e}"
            )
            raise

    def query(self, *args: t.Any, return_df: bool = False, **kwargs: t.Any) -> t.Any:
        """Execute a query against the test database.

        Args:
            *args: Arguments to pass to DuckDB's sql method
            **kwargs: Keyword arguments to pass to DuckDB's sql method

        Returns:
            For SELECT queries: Query results as a list of tuples
            For DDL/DML queries: None
        """
        with duckdb.connect(self.db_path) as conn:
            result = conn.sql(*args, **kwargs)
            # Only try to fetch results if it's a SELECT query
            if result is not None:
                if return_df:
                    return result.to_df()
                return result.fetchall()
            return None

    def initialize_test_source(self) -> None:
        conn = duckdb.connect(self.db_path)
        conn.sql(
            """
        CREATE SCHEMA sources;
        """
        )
        conn.sql(
            """
        CREATE TABLE sources.test_source (id INTEGER, name VARCHAR);
        """
        )
        conn.sql(
            """
        INSERT INTO sources.test_source (id, name)
        VALUES (1, 'abc'), (2, 'def');
        """
        )
        conn.close()

    def append_to_test_source(self, df: polars.DataFrame):
        logger.debug("appending data to the test source")
        conn = duckdb.connect(self.db_path)
        conn.sql(
            """
        INSERT INTO sources.test_source 
        SELECT * FROM df 
        """
        )

    def plan(
        self,
        *,
        environment: str,
        execution_time: TimeLike | None = None,
        enable_debug_console: bool = False,
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        plan_options: PlanOptions | None = None,
        restate_models: list[str] | None = None,
    ) -> None:
        """Runs plan and run on SQLMesh with the given configuration and record all of the generated events.

        Args:
            environment (str): The environment to run SQLMesh in.
            execution_time (TimeLike, optional): The execution timestamp for the run. Defaults to None.
            enable_debug_console (bool, optional): Flag to enable debug console. Defaults to False.
            start (TimeLike, optional): Start time for the run interval. Defaults to None.
            end (TimeLike, optional): End time for the run interval. Defaults to None.
            plan_options (PlanOptions, optional): Plan options for the plan. Defaults to None.
            restate_models (List[str], optional): List of models to restate. Defaults to None.

        Returns:
            None: The function records events to a debug console but doesn't return anything.

        Note:
            TimeLike can be any time-like object that SQLMesh accepts (datetime, str, etc.).
            The function creates a controller and recorder to capture all SQLMesh events during execution.
        """
        controller = self.create_controller(enable_debug_console=enable_debug_console)
        recorder = ConsoleRecorder()
        # controller.add_event_handler(ConsoleRecorder())
        if plan_options is None:
            plan_options = PlanOptions(
                enable_preview=True,
            )

        if execution_time:
            plan_options["execution_time"] = execution_time
        if restate_models:
            plan_options["restate_models"] = restate_models
        if start:
            plan_options["start"] = start
        if end:
            plan_options["end"] = end

        for event in controller.plan(
            environment,
            plan_options=plan_options,
            categorizer=None,
            default_catalog=None,
        ):
            recorder(event)

    def run(
        self,
        *,
        environment: str,
        execution_time: TimeLike | None = None,
        enable_debug_console: bool = False,
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        run_options: RunOptions | None = None,
        restate_models: list[str] | None = None,
    ) -> None:
        """Runs plan and run on SQLMesh with the given configuration and record all of the generated events.

        Args:
            environment (str): The environment to run SQLMesh in.
            execution_time (TimeLike, optional): The execution timestamp for the run. Defaults to None.
            enable_debug_console (bool, optional): Flag to enable debug console. Defaults to False.
            start (TimeLike, optional): Start time for the run interval. Defaults to None.
            end (TimeLike, optional): End time for the run interval. Defaults to None.
            run_options (RunOptions, optional): Run options for the run. Defaults to None.
            restate_models (List[str], optional): List of models to restate. Defaults to None.

        Returns:
            None: The function records events to a debug console but doesn't return anything.

        Note:
            TimeLike can be any time-like object that SQLMesh accepts (datetime, str, etc.).
            The function creates a controller and recorder to capture all SQLMesh events during execution.
        """
        controller = self.create_controller(enable_debug_console=enable_debug_console)
        recorder = ConsoleRecorder()
        # controller.add_event_handler(ConsoleRecorder())
        if run_options is None:
            run_options = RunOptions()

        if execution_time:
            run_options["execution_time"] = execution_time
        if start:
            run_options["start"] = start
        if end:
            run_options["end"] = end

        for event in controller.run(
            environment,
            **run_options,
        ):
            recorder(event)

    def plan_and_run(
        self,
        *,
        environment: str,
        enable_debug_console: bool = False,
        start: TimeLike | None = None,
        end: TimeLike | None = None,
        select_models: list[str] | None = None,
        restate_selected: bool = False,
        skip_run: bool = False,
        plan_options: PlanOptions | None = None,
        run_options: RunOptions | None = None,
    ) -> None:
        """Runs plan and run on SQLMesh with the given configuration and record all of the generated events.

        Args:
            environment (str): The environment to run SQLMesh in.
            enable_debug_console (bool, optional): Flag to enable debug console. Defaults to False.
            start (TimeLike, optional): Start time for the run interval. Defaults to None.
            end (TimeLike, optional): End time for the run interval. Defaults to None.
            execution_time (TimeLike, optional): The execution timestamp for the run. Defaults to None.
            select_models (List[str], optional): List of models to select. Defaults to None.
            restate_selected (bool, optional): Flag to restate selected models. Defaults to False.
            skip_run (bool, optional): Flag to skip the run. Defaults to False.
            plan_options (PlanOptions, optional): Plan options for the plan. Defaults to None.
            run_options (RunOptions, optional): Run options for the run. Defaults to None.

        Returns:
            None: The function records events to a debug console but doesn't return anything.

        Note:
            TimeLike can be any time-like object that SQLMesh accepts (datetime, str, etc.).
            The function creates a controller and recorder to capture all SQLMesh events during execution.
        """
        controller = self.create_controller(enable_debug_console=enable_debug_console)
        recorder = ConsoleRecorder()

        plan_options = plan_options or PlanOptions(enable_preview=True)
        run_options = run_options or RunOptions()

        if plan_options.get("select_models") or run_options.get("select_models"):
            raise ValueError(
                "select_models should not be set in plan_options or run_options use the `select_models` or `select_models_func` arguments instead"
            )
        if plan_options.get("restate_models"):
            raise ValueError(
                "restate_models should not be set in plan_options use the `restate_selected` argument with `select_models` or `select_models_func` instead"
            )

        for event in controller.plan_and_run(
            environment,
            start=start,
            end=end,
            select_models=select_models,
            restate_selected=restate_selected,
            plan_options=plan_options,
            run_options=run_options,
            skip_run=skip_run,
        ):
            recorder(event)


@pytest.fixture
def sample_sqlmesh_test_context(
    sample_sqlmesh_project: str,
) -> t.Iterator[SQLMeshTestContext]:
    db_path = os.path.join(sample_sqlmesh_project, "db.db")
    config = SQLMeshConfig(
        gateways={
            "local": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
        },
        default_gateway="local",
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    config_as_dict = config.dict()
    context_config = SQLMeshContextConfig(
        path=sample_sqlmesh_project, gateway="local", config_override=config_as_dict
    )
    test_context = SQLMeshTestContext(
        db_path=db_path,
        context_config=context_config,
        project_path=sample_sqlmesh_project,
    )
    test_context.initialize_test_source()
    yield test_context


@pytest.fixture
def permanent_sqlmesh_project() -> str:
    """FOR DEBUGGING ONLY: Returns the path to the permanent sample SQLMesh project.

    This fixture provides access to the sample project without copying to a temp directory,
    which is useful for debugging and investigating issues with file handling.
    It creates a permanent copy of the sample project in tests/temp/sqlmesh_project
    if it doesn't exist.

    Returns:
        str: Absolute path to the sample SQLMesh project directory
    """
    # Define source and target paths
    source_dir = os.path.abspath("sample/sqlmesh_project")
    project_dir = os.path.abspath("tests/temp/sqlmesh_project")

    # Create the temp directory if it doesn't exist
    os.makedirs(os.path.dirname(project_dir), exist_ok=True)

    # If project directory doesn't exist or is empty, copy from sample
    if not os.path.exists(project_dir) or not os.listdir(project_dir):
        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)
        shutil.copytree(source_dir, project_dir)

    # Clean up any existing db file
    db_path = os.path.join(project_dir, "db.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    return project_dir


@pytest.fixture
def model_change_test_context(
    permanent_sqlmesh_project: str,
) -> t.Iterator[SQLMeshTestContext]:
    """FOR DEBUGGING ONLY: Creates a SQLMesh test context specifically for testing model code changes.

    This fixture provides a context that allows modifying SQL model files and ensures
    they are properly restored after the test completes. It uses a permanent project
    directory instead of a temporary one for better debugging and investigation.

    Args:
        permanent_sqlmesh_project: The permanent project directory

    Yields:
        SQLMeshTestContext: A test context with additional methods for modifying model files
    """
    db_path = os.path.join(permanent_sqlmesh_project, "db.db")
    config = SQLMeshConfig(
        gateways={
            "local": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
        },
        default_gateway="local",
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    config_as_dict = config.dict()
    context_config = SQLMeshContextConfig(
        path=permanent_sqlmesh_project, gateway="local", config_override=config_as_dict
    )
    test_context = SQLMeshTestContext(
        db_path=db_path,
        context_config=context_config,
        project_path=permanent_sqlmesh_project,
    )
    test_context.initialize_test_source()

    yield test_context

    # Cleanup: restore any modified files
    # test_context.cleanup_modified_files()


@dataclass
class DagsterTestContext:
    """A test context for running Dagster"""

    dagster_project_path: str
    sqlmesh_project_path: str

    def _run_command(self, cmd: list[str]) -> None:
        """Execute a command and stream its output in real-time.

        Args:
            cmd: List of command parts to execute

        Raises:
            subprocess.CalledProcessError: If the command returns non-zero exit code
        """
        import io
        import queue
        import threading
        import typing as t

        def stream_output(
            pipe: t.IO[str], output_queue: queue.Queue[tuple[str, str | None]]
        ) -> None:
            """Stream output from a pipe to a queue.

            Args:
                pipe: The pipe to read from (stdout or stderr)
                output_queue: Queue to write output to, as (stream_type, line) tuples
            """
            # Use a StringIO buffer to accumulate characters into lines
            buffer = io.StringIO()
            stream_type = "stdout" if pipe is process.stdout else "stderr"

            try:
                while True:
                    char = pipe.read(1)
                    if not char:
                        # Flush any remaining content in buffer
                        remaining = buffer.getvalue()
                        if remaining:
                            output_queue.put((stream_type, remaining))
                        break

                    buffer.write(char)

                    # If we hit a newline, flush the buffer
                    if char == "\n":
                        output_queue.put((stream_type, buffer.getvalue()))
                        buffer = io.StringIO()
            finally:
                buffer.close()
                output_queue.put((stream_type, None))  # Signal EOF

        print(f"Running command: {' '.join(cmd)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Changing to directory: {self.dagster_project_path}")

        # Change to the dagster project directory before running the command
        os.chdir(self.dagster_project_path)

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            universal_newlines=True,
            encoding="utf-8",
            errors="replace",
        )

        if not process.stdout or not process.stderr:
            raise RuntimeError("Failed to open subprocess pipes")

        # Create a single queue for all output
        output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()

        # Start threads to read from pipes
        stdout_thread = threading.Thread(
            target=stream_output, args=(process.stdout, output_queue)
        )
        stderr_thread = threading.Thread(
            target=stream_output, args=(process.stderr, output_queue)
        )

        stdout_thread.daemon = True
        stderr_thread.daemon = True
        stdout_thread.start()
        stderr_thread.start()

        # Track which streams are still active
        active_streams = {"stdout", "stderr"}

        # Read from queue and print output
        while active_streams:
            try:
                stream_type, content = output_queue.get(timeout=0.1)
                if content is None:
                    active_streams.remove(stream_type)
                else:
                    print(content, end="", flush=True)
            except queue.Empty:
                continue

        stdout_thread.join()
        stderr_thread.join()
        process.wait()

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)

    def asset_materialisation(
        self,
        assets: list[str],
        plan_options: PlanOptions | None = None,
        run_options: RunOptions | None = None,
    ) -> None:
        """Materialises the given Dagster assets using CLI command.

        Args:
            assets: String of comma-separated asset names to materialize
            plan_options: Optional SQLMesh plan options to pass to the config
            run_options: Optional SQLMesh run options to pass to the config
        """
        config: dict[str, t.Any] = {
            "resources": {
                "sqlmesh": {
                    "config": {
                        "config": {
                            "gateway": "local",
                            "path": self.sqlmesh_project_path,
                        }
                    }
                }
            }
        }

        # Add plan options if provided
        if plan_options:
            config["resources"]["sqlmesh"]["config"]["plan_options_override"] = {
                k: v for k, v in plan_options.items() if v is not None
            }

        if run_options:
            config["resources"]["sqlmesh"]["config"]["run_options_override"] = {
                k: v for k, v in run_options.items() if v is not None
            }

        # Convert config to JSON string, escaping backslashes for Windows paths
        config_json = json.dumps(config).replace("\\", "\\\\")

        cmd = [
            sys.executable,
            "-m",
            "dagster",
            "asset",
            "materialize",
            "-f",
            os.path.join(self.dagster_project_path, "definitions.py"),
            "--select",
            ",".join(assets),
            "--config-json",
            config_json,
        ]

        self._run_command(cmd)

    def reset_assets(self) -> None:
        """Resets the assets to the original state"""
        self.asset_materialisation(assets=["reset_asset"])

    def init_test_source(self) -> None:
        """Initialises the test source"""
        self.asset_materialisation(assets=["test_source"])


@pytest.fixture
def sample_dagster_test_context(
    sample_dagster_project: str,
) -> t.Iterator[DagsterTestContext]:
    test_context = DagsterTestContext(
        dagster_project_path=os.path.join(sample_dagster_project),
        sqlmesh_project_path=os.path.join(
            sample_dagster_project.replace("dagster_project", "sqlmesh_project")
        ),
    )
    yield test_context


if __name__ == "__main__":
    pytest.main([__file__])
