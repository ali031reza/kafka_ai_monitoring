import argparse
from pathlib import Path

from config.config_manager import ConfigManager
from src.database.client import DatabaseClient


EXPECTED_MODELS = (
    "car",
    "fire",
    "person_face",
    "ppe",
)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Kafka AI Monitoring result database integrity"
    )

    parser.add_argument(
        "--since-minutes",
        type=int,
        default=120,
        help="Inspect records created during the last N minutes",
    )

    args = parser.parse_args()

    if args.since_minutes <= 0:
        parser.error("--since-minutes must be positive")

    root = Path(__file__).resolve().parents[2]

    config = ConfigManager(root)

    database = DatabaseClient(
        **config.get_database_config()
    )

    print("=" * 65)
    print("KAFKA AI MONITORING - DATABASE INTEGRITY VERIFICATION")
    print("=" * 65)

    # --------------------------------------------------
    # 1. Overall inference result counts
    # --------------------------------------------------

    summary = database.fetch_one(
        """
        SELECT
            COUNT(*)::bigint,

            COUNT(*) FILTER (
                WHERE is_event = TRUE
            )::bigint,

            COUNT(*) FILTER (
                WHERE is_event = TRUE
                  AND event_image_id IS NOT NULL
            )::bigint,

            COUNT(*) FILTER (
                WHERE is_event = FALSE
                  AND event_image_id IS NOT NULL
            )::bigint

        FROM inference_results

        WHERE created_at >=
            NOW() - (%s * INTERVAL '1 minute')
        """,
        (args.since_minutes,),
    )

    total, events, linked_events, invalid_non_events = summary

    print("\n1. INFERENCE RESULTS")

    print(f"Total results: {total}")
    print(f"Event results: {events}")
    print(f"Events linked to images: {linked_events}")
    print(
        "Non-events incorrectly linked to images: "
        f"{invalid_non_events}"
    )

    # --------------------------------------------------
    # 2. Per-model counts
    # --------------------------------------------------

    model_rows = database.fetch_all(
        """
        SELECT
            model_id,
            COUNT(*)::bigint,

            COUNT(*) FILTER (
                WHERE is_event = TRUE
            )::bigint

        FROM inference_results

        WHERE created_at >=
            NOW() - (%s * INTERVAL '1 minute')

        GROUP BY model_id
        ORDER BY model_id
        """,
        (args.since_minutes,),
    )

    print("\n2. MODEL DISTRIBUTION")

    observed_models = set()

    for model_id, count, event_count in model_rows:
        observed_models.add(model_id)

        event_rate = (
            event_count / count * 100
            if count else 0
        )

        print(
            f"{model_id:15} "
            f"results={count:8} "
            f"events={event_count:8} "
            f"event_rate={event_rate:.2f}%"
        )

    for model in EXPECTED_MODELS:
        if model not in observed_models:
            print(
                f"WARNING: No recent records for {model}"
            )

    # --------------------------------------------------
    # 3. Validate event-image relationships
    # --------------------------------------------------

    image_mismatches = database.fetch_one(
        """
        SELECT COUNT(*)::bigint

        FROM inference_results ir

        LEFT JOIN event_images ei
            ON ei.id = ir.event_image_id

        WHERE ir.created_at >=
            NOW() - (%s * INTERVAL '1 minute')

          AND (
              (
                  ir.is_event = TRUE

                  AND (
                      ir.event_image_id IS NULL
                      OR ei.id IS NULL
                      OR ei.camera_id IS DISTINCT FROM ir.camera_id
                      OR ei.frame_id IS DISTINCT FROM ir.frame_id
                  )
              )

              OR

              (
                  ir.is_event = FALSE
                  AND ir.event_image_id IS NOT NULL
              )
          )
        """,
        (args.since_minutes,),
    )[0]

    print("\n3. EVENT IMAGE RELATIONSHIPS")
    print(f"Invalid image relationships: {image_mismatches}")

    # --------------------------------------------------
    # 4. Check orphan event images
    # --------------------------------------------------

    orphan_images = database.fetch_one(
        """
        SELECT COUNT(*)::bigint

        FROM event_images ei

        WHERE ei.stored_at >=
            NOW() - (%s * INTERVAL '1 minute')

          AND NOT EXISTS (
              SELECT 1

              FROM inference_results ir

              WHERE ir.event_image_id = ei.id
          )
        """,
        (args.since_minutes,),
    )[0]

    print(f"Orphan event images: {orphan_images}")

    # --------------------------------------------------
    # 5. Check duplicate inference results
    # --------------------------------------------------

    duplicates = database.fetch_one(
        """
        SELECT COUNT(*)::bigint

        FROM (
            SELECT
                model_id,
                frame_id

            FROM inference_results

            WHERE created_at >=
                NOW() - (%s * INTERVAL '1 minute')

            GROUP BY model_id, frame_id

            HAVING COUNT(*) > 1
        ) duplicates
        """,
        (args.since_minutes,),
    )[0]

    print("\n4. DUPLICATE PROTECTION")
    print(f"Duplicate inference records: {duplicates}")

    # --------------------------------------------------
    # 6. Final assessment
    # --------------------------------------------------

    print("\n" + "=" * 65)

    failed_checks = (
        invalid_non_events
        + image_mismatches
        + orphan_images
        + duplicates
    )

    if total == 0:
        print("WARNING: No recent inference records found.")
        print("Check the processor and the selected time window.")
        raise SystemExit(2)

    if failed_checks:
        print("INTEGRITY CHECK FAILED")
        print(f"Detected inconsistencies: {failed_checks}")
        raise SystemExit(1)

    print("DATABASE INTEGRITY CHECK PASSED")
    print("No duplicate or inconsistent records detected.")

    print("=" * 65)


if __name__ == "__main__":
    main()