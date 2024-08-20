from typing import Any, List, Optional, Set, Callable
import logging

from sqlmesh.core.model import Model
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.snapshot import SnapshotInfoLike, SnapshotId
from sqlmesh.core.plan import Plan

from dagster_sqlmesh import console

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def show_plan_summary(
    plan: Plan,
    snapshot_selector: Callable[[SnapshotInfoLike], bool],
    ignored_snapshot_ids: Optional[Set[SnapshotId]] = None,
):
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
    restated_snapshots: List[SnapshotInfoLike] = [
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


class StatefulConsoleEventHandler:
    def __init__(self, enable_unknown_event_logging: bool = True):
        self._planned_models: List[Model] = []
        self._enable_unknown_event_logging = enable_unknown_event_logging

    def __call__(self, event: console.ConsoleEvent):
        match event:
            case console.StartPlanEvaluation(plan):
                logger.debug("Starting plan evaluation")
                self._show_summary_for(
                    plan,
                    lambda x: x.is_model,
                )
            case console.StartEvaluationProgress(
                batches, environment_naming_info, default_catalog
            ):
                logger.debug("STARTING EVALUATION")
                logger.debug(batches)
                logger.debug(environment_naming_info)
                logger.debug(default_catalog)

            case console.UpdatePromotionProgress(snapshot, promoted):
                logger.debug("UPDATE PROMOTION PROGRESS")
                logger.debug(snapshot)
                logger.debug(promoted)
            case console.StopPromotionProgress(success):
                logger.debug("STOP PROMOTION")
            case console.StartSnapshotEvaluationProgress(snapshot):
                logger.debug("START SNAPSHOT EVALUATION")
                logger.debug(snapshot.name)
            case console.UpdateSnapshotEvaluationProgress(
                snapshot, batch_idx, duration_ms
            ):
                logger.debug("UPDATE SNAPSHOT EVALUATION")
                logger.debug(snapshot.name)
            case _:
                if self._enable_unknown_event_logging:
                    logger.debug("Unhandled event")
                    logger.debug(event)

    def _show_summary_for(
        self,
        plan: Plan,
        snapshot_selector: Callable[[SnapshotInfoLike], bool],
        ignored_snapshot_ids: Optional[Set[SnapshotId]] = None,
    ):
        return show_plan_summary(plan, snapshot_selector, ignored_snapshot_ids)
