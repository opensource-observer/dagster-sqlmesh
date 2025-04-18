import typing as t

from sqlmesh.core.scheduler import Scheduler
from sqlmesh.utils import CompletionStatus


class DagsterSQLMeshScheduler(Scheduler):
    """Custom Scheduler so that we can choose a set of snapshots to use with
    sqlmesh runs"""

    def __init__(
        self, selected_snapshots: set[str] | None = None, *args: t.Any, **kwargs: t.Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self._selected_snapshots: set[str] = selected_snapshots or set()

    def run(self, *args: t.Any, **kwargs: t.Any) -> CompletionStatus:
        if len(self._selected_snapshots) > 0:
            kwargs["selected_snapshots"] = self._selected_snapshots
        return super().run(*args, **kwargs)
