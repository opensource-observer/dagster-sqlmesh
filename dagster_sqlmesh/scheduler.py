import typing as t

from sqlmesh.core.scheduler import Scheduler


class DagsterSQLMeshScheduler(Scheduler):
    """Custom Scheduler so that we can choose a set of snapshots to use with
    sqlmesh runs"""

    def __init__(self, selected_snapshots: t.Optional[t.Set[str]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._selected_snapshots: t.Set[str] = selected_snapshots or set()

    def run(self, *args, **kwargs) -> bool:
        if len(self._selected_snapshots) > 0:
            kwargs["selected_snapshots"] = self._selected_snapshots
        return super().run(*args, **kwargs)
