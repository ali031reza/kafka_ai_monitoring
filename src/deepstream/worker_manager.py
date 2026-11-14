"""Deterministic source assignment for per-model worker replicas.

This module plans worker ownership; it does not launch DeepStream processes.
A camera/source is assigned to one worker replica for a given model.
"""

from dataclasses import dataclass
from collections.abc import Sequence


@dataclass(frozen=True)
class WorkerAssignment:
    model_id: str
    worker_id: str
    source_ids: tuple[str, ...]


def plan_model_workers(
    model_id: str,
    worker_count: int,
    source_ids: Sequence[str],
) -> tuple[WorkerAssignment, ...]:
    """Distribute distinct sources round-robin across model worker replicas.

    The source list is expected to contain project camera IDs or stable
    source keys. Splitting one camera's individual frames across workers is
    a different design and is intentionally not implemented here.
    """
    if not isinstance(model_id, str) or not model_id.strip():
        raise ValueError("model_id must be a non-empty string")
    if isinstance(worker_count, bool) or not isinstance(worker_count, int):
        raise ValueError("worker_count must be an integer")
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")

    normalized_sources = tuple(source_ids)
    if any(not isinstance(source_id, str) or not source_id.strip()
           for source_id in normalized_sources):
        raise ValueError("source IDs must be non-empty strings")
    if len(set(normalized_sources)) != len(normalized_sources):
        raise ValueError("source IDs must be unique")

    if not normalized_sources:
        return ()
    if worker_count > len(normalized_sources):
        raise ValueError(
            "worker_count cannot exceed the number of distinct sources "
            "for source-based worker assignment"
        )

    buckets: list[list[str]] = [[] for _ in range(worker_count)]
    for index, source_id in enumerate(normalized_sources):
        buckets[index % worker_count].append(source_id)

    return tuple(
        WorkerAssignment(
            model_id=model_id,
            worker_id=f"{model_id}-worker-{index + 1:02d}",
            source_ids=tuple(assigned_sources),
        )
        for index, assigned_sources in enumerate(buckets)
    )


def build_worker_plan(
    model_configs: dict,
    source_ids_by_model: dict[str, Sequence[str]],
) -> dict[str, tuple[WorkerAssignment, ...]]:
    """Build assignments for enabled models using independent worker counts."""
    plan: dict[str, tuple[WorkerAssignment, ...]] = {}
    for model_id, model_config in model_configs.items():
        if not model_config.enabled:
            continue
        plan[model_id] = plan_model_workers(
            model_id=model_id,
            worker_count=model_config.workers,
            source_ids=source_ids_by_model.get(model_id, ()),
        )
    return plan
