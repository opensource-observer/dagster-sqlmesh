import io
import logging
import typing as t
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import dagster as dg
import pytest

from dagster_sqlmesh.config import SQLMeshContextConfig
from dagster_sqlmesh.resource import SQLMeshResource


@pytest.mark.parametrize("level,expected_renders", [(logging.INFO, 0), (logging.DEBUG, 1)])
def test_run_formats_events_only_when_debug_logging_is_enabled(
    monkeypatch: pytest.MonkeyPatch, level: int, expected_renders: int
) -> None:
    class Event:
        renders = 0

        def __str__(self) -> str:
            self.renders += 1
            return "fixture event"

    event = Event()
    controller = MagicMock()
    mesh = controller.instance.return_value.__enter__.return_value
    mesh.models.return_value = {}
    mesh.non_external_models_dag.return_value = []
    mesh.plan_and_run.return_value = iter([event])
    handler = Mock()
    handler.errors = []
    handler.notify_success.return_value = iter(())
    monkeypatch.setattr(SQLMeshResource, "get_controller", Mock(return_value=controller))
    monkeypatch.setattr(SQLMeshResource, "create_event_handler", Mock(return_value=handler))
    output = io.StringIO()
    logger = logging.Logger("event-formatting", level=level)
    logger.addHandler(logging.StreamHandler(output))
    context = t.cast(
        dg.AssetExecutionContext,
        SimpleNamespace(
            log=logger, op_execution_context=SimpleNamespace(selected_output_names=[])
        ),
    )
    config = SQLMeshContextConfig(path=".", gateway="unused")

    assert list(SQLMeshResource().run(context=context, config=config)) == []
    handler.process_events.assert_called_once_with(event)
    assert event.renders == expected_renders
    assert ("sqlmesh event: fixture event" in output.getvalue()) == bool(expected_renders)
