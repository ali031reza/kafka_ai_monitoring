import argparse
import json
import statistics
import time
from pathlib import Path
from uuid import uuid4

import psycopg

from config.config_manager import ConfigManager
from src.database.image_repository import ImageRepository
from src.database.results_repository import ResultsRepository
from src.proto.inference_result_pb2 import InferenceResult


EVENT_TYPES = {
    "car": "vehicle_detected",
    "fire": "fire_detected",
    "person_face": "person_detected",
    "ppe": "ppe_violation",
}


def _percentile(values, percentile):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1,
        max(0, int(round((percentile / 100.0) * (len(ordered) - 1)))),
    )
    return ordered[index]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark PostgreSQL result and event-image metadata "
            "inserts using the application's repositories"
        )
    )
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument(
        "--models",
        default="car,fire,person_face,ppe",
        help="Comma-separated model IDs",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help=(
            "Commit benchmark rows to measure durable transaction "
            "throughput, then delete only this run's tagged rows"
        ),
    )
    parser.add_argument(
        "--output",
        default="artifacts/load_test/postgres_benchmark.json",
    )
    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be positive")

    models = [
        item.strip()
        for item in args.models.split(",")
        if item.strip()
    ]
    if not models or any(model not in EVENT_TYPES for model in models):
        parser.error(
            f"--models must contain one or more of: {', '.join(EVENT_TYPES)}"
        )

    root = Path(__file__).resolve().parents[2]
    config = ConfigManager(root)
    database_config = config.get_database_config()

    image_repository = ImageRepository()
    results_repository = ResultsRepository()

    run_tag = uuid4().hex[:10]
    camera_prefix = f"LT{run_tag}_"
    image_sha256 = "a" * 64
    transaction_times_ms = []
    event_count = 0
    completed = 0
    started = time.perf_counter()
    measurement_elapsed = None
    cleanup_completed = False

    print(
        f"[POSTGRES BENCHMARK] rows={args.count}, "
        f"models={models}, commit={args.commit}, "
        f"run_tag={run_tag}"
    )
    if not args.commit:
        print(
            "[POSTGRES BENCHMARK] Rollback mode: measures SQL execution "
            "and connection overhead, but not durable commit throughput."
        )
    else:
        print(
            "[POSTGRES BENCHMARK] Commit mode: tagged rows are committed "
            "and automatically removed after measurement."
        )

    try:
        for index in range(args.count):
            model_id = models[index % len(models)]
            is_event = index % 2 == 0
            frame_id = f"{run_tag}-{index:010d}"
            camera_id = f"{camera_prefix}{index % 40:02d}"
            now_ms = time.time_ns() // 1_000_000

            result = InferenceResult(
                camera_id=camera_id,
                frame_id=frame_id,
                model_id=model_id,
                processed_at_ms=now_ms,
                status=(
                    "load_test_event"
                    if is_event
                    else "load_test_no_event"
                ),
                is_event=is_event,
            )

            if is_event:
                result.event_type = EVENT_TYPES[model_id]
                result.storage_key = (
                    f"load-test/{run_tag}/{model_id}/{frame_id}.jpg"
                )
                result.image_format = "jpeg"
                result.image_size_bytes = 33268
                result.image_sha256 = image_sha256
                result.captured_at_ms = now_ms
                event_count += 1

            transaction_started = time.perf_counter()
            with psycopg.connect(**database_config) as connection:
                with connection.cursor() as cursor:
                    event_image_id = None
                    if is_event:
                        event_image_id = image_repository.save(
                            cursor=cursor,
                            result=result,
                        )
                    results_repository.save(
                        cursor=cursor,
                        result=result,
                        event_image_id=event_image_id,
                    )

                if not args.commit:
                    connection.rollback()

            transaction_times_ms.append(
                (time.perf_counter() - transaction_started) * 1000.0
            )
            completed += 1

            if completed % 250 == 0:
                elapsed = max(time.perf_counter() - started, 1e-9)
                print(
                    f"[POSTGRES PROGRESS] rows={completed}, "
                    f"rate={completed / elapsed:.1f} rows/s"
                )

        # Stop the measured interval before cleanup deletes benchmark rows.
        measurement_elapsed = time.perf_counter() - started
    finally:
        if args.commit:
            # Remove only rows created by this invocation. Inference rows
            # reference event_images, so delete them first.
            try:
                with psycopg.connect(**database_config) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            DELETE FROM inference_results
                            WHERE left(camera_id, %s) = %s
                            """,
                            (len(camera_prefix), camera_prefix),
                        )
                        deleted_results = cursor.rowcount

                        cursor.execute(
                            """
                            DELETE FROM event_images
                            WHERE left(camera_id, %s) = %s
                            """,
                            (len(camera_prefix), camera_prefix),
                        )
                        deleted_images = cursor.rowcount
                cleanup_completed = True
                print(
                    f"[POSTGRES CLEANUP] results={deleted_results}, "
                    f"event_images={deleted_images}"
                )
            except Exception as cleanup_error:
                print(
                    "[POSTGRES CLEANUP ERROR] "
                    f"run_tag={run_tag}, error={cleanup_error}"
                )
                print(
                    "Run the cleanup SQL from docs/LOAD_TESTING.md "
                    "after reviewing the run tag."
                )

    elapsed = max(
        measurement_elapsed
        if measurement_elapsed is not None
        else time.perf_counter() - started,
        1e-9,
    )
    result = {
        "run_tag": run_tag,
        "mode": "commit_and_cleanup" if args.commit else "rollback",
        "requested_rows": args.count,
        "completed_rows": completed,
        "event_image_metadata_rows_attempted": event_count,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(completed / elapsed, 3),
        "transaction_avg_ms": round(
            statistics.mean(transaction_times_ms)
            if transaction_times_ms else 0.0,
            3,
        ),
        "transaction_p50_ms": round(
            _percentile(transaction_times_ms, 50),
            3,
        ),
        "transaction_p95_ms": round(
            _percentile(transaction_times_ms, 95),
            3,
        ),
        "cleanup_completed": cleanup_completed if args.commit else None,
        "durable_commit_measured": bool(args.commit),
    }

    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    print("[POSTGRES BENCHMARK RESULT]")
    print(json.dumps(result, indent=2))
    print(f"JSON report saved: {output}")


if __name__ == "__main__":
    main()
