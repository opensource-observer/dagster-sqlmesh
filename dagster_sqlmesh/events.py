import logging
import queue
import threading
from collections.abc import Callable, Iterator

from sqlmesh.core.model import Model
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotInfoLike

from dagster_sqlmesh import console

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def show_plan_summary(
    logger: logging.Logger,
    plan: Plan,
    snapshot_selector: Callable[[SnapshotInfoLike], bool],
    ignored_snapshot_ids: set[SnapshotId] | None = None,
) -> None:
    context_diff = plan.context_diff
    ignored_snapshot_ids = ignored_snapshot_ids or set()
    selected_snapshots = {
        s_id: snapshot
        for s_id, snapshot in context_diff.snapshots.items()
        if snapshot_selector(snapshot)
    }
    selected_ignored_snapshot_ids = {
        s_id for s_id in selected_snapshots if s_id in ignored_snapshot_ids
    }
    added_snapshot_ids = {
        s_id
        for s_id in context_diff.added
        if snapshot_selector(context_diff.snapshots[s_id])
    } - selected_ignored_snapshot_ids
    removed_snapshot_ids = {
        s_id
        for s_id, snapshot in context_diff.removed_snapshots.items()
        if snapshot_selector(snapshot)
    } - selected_ignored_snapshot_ids
    modified_snapshot_ids = {
        current_snapshot.snapshot_id
        for _, (current_snapshot, _) in context_diff.modified_snapshots.items()
        if snapshot_selector(current_snapshot)
    } - selected_ignored_snapshot_ids
    restated_snapshots: list[SnapshotInfoLike] = [
        context_diff.snapshots[snap_id] for snap_id in plan.restatements.keys()
    ]

    logger.debug("==================================")
    logger.debug(plan)
    logger.debug("==================================")
    logger.debug("Added")
    logger.debug(added_snapshot_ids)
    logger.debug("Removed")
    logger.debug(removed_snapshot_ids)
    logger.debug("Modified")
    logger.debug(modified_snapshot_ids)
    logger.debug("restated_snapshots")
    logger.debug(restated_snapshots)


class ConsoleGenerator:
    def __init__(self, log_override: logging.Logger | None = None):
        self._queue: queue.Queue[console.ConsoleEvent] = queue.Queue()
        self.logger = log_override or logger

    def __call__(self, event: console.ConsoleEvent) -> None:
        self._queue.put(event)

    def events(self, thread: threading.Thread) -> Iterator[console.ConsoleEvent]:
        while thread.is_alive() or not self._queue.empty():
            try:
                # Get arguments from the queue with a timeout
                args = self._queue.get(timeout=0.5)
                yield args
            except queue.Empty:
                continue


class ConsoleRecorder:
    def __init__(
        self,
        log_override: logging.Logger | None = None,
        enable_unknown_event_logging: bool = True,
    ):
        self.logger = log_override or logger
        self._planned_models: list[Model] = []
        self._updated: list[Snapshot] = []
        self._successful = False
        self._enable_unknown_event_logging = enable_unknown_event_logging

    def __call__(self, event: console.ConsoleEvent) -> None:
        match event:
            case console.StartPlanEvaluation(plan=evaluatable_plan):
                self.logger.debug("Starting plan evaluation")
                print(evaluatable_plan.plan_id)
            case console.StartEvaluationProgress(
                batched_intervals=batches, environment_naming_info=environment_naming_info, default_catalog=default_catalog
            ):
                self.logger.debug("STARTING EVALUATION")
                self.logger.debug(batches)
                self.logger.debug(environment_naming_info)
                self.logger.debug(default_catalog)
            case console.UpdatePromotionProgress(snapshot=snapshot, promoted=promoted):
                self.logger.debug("UPDATE PROMOTION PROGRESS")
                self.logger.debug(snapshot)
                self.logger.debug(promoted)
            case console.StopPromotionProgress(success=success):
                self.logger.debug("STOP PROMOTION")
                self.logger.debug(success)
                self._successful = True
            case console.StartSnapshotEvaluationProgress(snapshot=snapshot):
                self.logger.debug("START SNAPSHOT EVALUATION")
                self.logger.debug(snapshot.name)
            case console.UpdateSnapshotEvaluationProgress(
                snapshot=snapshot, batch_idx=batch_idx, duration_ms=duration_ms
            ):
                self._updated.append(snapshot)
                self.logger.debug("UPDATE SNAPSHOT EVALUATION")
                self.logger.debug(snapshot.name)
                self.logger.debug(batch_idx)
                self.logger.debug(duration_ms)
            case _:
                if self._enable_unknown_event_logging:
                    self.logger.debug(f"Unhandled event {event.__class__.__name__}")
                    self.logger.debug(event)

    def _show_summary_for(
        self,
        plan: Plan,
        snapshot_selector: Callable[[SnapshotInfoLike], bool],
        ignored_snapshot_ids: set[SnapshotId] | None = None,
    ) -> None:
        return show_plan_summary(
            self.logger, plan, snapshot_selector, ignored_snapshot_ids
        )
