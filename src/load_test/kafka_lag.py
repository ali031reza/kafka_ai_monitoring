import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from config.config_manager import ConfigManager


def _targets(kafka_config: dict) -> list[dict]:
    targets = []
    for model_id, settings in kafka_config["topics"].items():
        targets.append({
            "group_id": settings["group_id"],
            "stream_type": "frames",
            "model": model_id,
            "topic": settings["frames"],
        })

    results_group = kafka_config.get(
        "results_consumer", {}
    ).get("group_id", "results_processor_group")

    for model_id, settings in kafka_config["topics"].items():
        targets.append({
            "group_id": results_group,
            "stream_type": "results",
            "model": model_id,
            "topic": settings["results"],
        })
    return targets


def _snapshot_target(bootstrap_servers, target):
    consumer = Consumer({
        "bootstrap.servers": ",".join(bootstrap_servers),
        "group.id": target["group_id"],
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "client.id": f"load-test-lag-{target['group_id']}",
    })

    rows = []
    try:
        metadata = consumer.list_topics(
            target["topic"],
            timeout=10,
        )
        topic_metadata = metadata.topics.get(target["topic"])
        if topic_metadata is None:
            raise RuntimeError(
                f"Topic not found: {target['topic']}"
            )
        if topic_metadata.error is not None:
            raise RuntimeError(
                f"Kafka metadata error for {target['topic']}: "
                f"{topic_metadata.error}"
            )

        partition_ids = sorted(topic_metadata.partitions)
        requested = [
            TopicPartition(target["topic"], partition_id)
            for partition_id in partition_ids
        ]
        committed = consumer.committed(
            requested,
            timeout=10,
        )

        for partition in committed:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(
                    partition.topic,
                    partition.partition,
                ),
                timeout=10,
                cached=False,
            )
            committed_offset = partition.offset
            effective_offset = (
                committed_offset if committed_offset >= 0 else low
            )
            lag = max(0, high - effective_offset)

            rows.append({
                **target,
                "partition": partition.partition,
                "low_offset": low,
                "high_offset": high,
                "committed_offset": committed_offset,
                "lag": lag,
            })
    finally:
        consumer.close()

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Sample Kafka consumer-group lag for AI model and results groups"
    )
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument(
        "--output",
        default="artifacts/load_test/kafka_lag.csv",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.duration <= 0:
        parser.error("--duration must be positive")

    root = Path(__file__).resolve().parents[2]
    config = ConfigManager(root)
    kafka = config.get_section("kafka")
    targets = _targets(kafka)

    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        "timestamp_utc",
        "group_id",
        "stream_type",
        "model",
        "topic",
        "partition",
        "low_offset",
        "high_offset",
        "committed_offset",
        "lag",
    ]

    started = time.monotonic()
    sample_number = 0

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()

        try:
            while time.monotonic() - started < args.duration:
                timestamp = datetime.now(timezone.utc).isoformat()
                sample_rows = []

                for target in targets:
                    sample_rows.extend(
                        _snapshot_target(
                            kafka["bootstrap_servers"],
                            target,
                        )
                    )

                for row in sample_rows:
                    writer.writerow({
                        "timestamp_utc": timestamp,
                        **row,
                    })
                handle.flush()

                totals = {}
                for row in sample_rows:
                    key = (
                        row["group_id"],
                        row["stream_type"],
                        row["model"] if row["stream_type"] == "frames" else "all",
                    )
                    totals[key] = totals.get(key, 0) + row["lag"]

                summary = ", ".join(
                    f"{group}/{stream}/{model}={lag}"
                    for (group, stream, model), lag in sorted(totals.items())
                )
                sample_number += 1
                print(
                    f"[KAFKA_LAG] sample={sample_number} "
                    f"timestamp={timestamp} {summary}",
                    flush=True,
                )

                remaining = args.duration - (
                    time.monotonic() - started
                )
                if remaining > 0:
                    time.sleep(min(args.interval, remaining))
        except KeyboardInterrupt:
            print("\nKafka lag sampling stopped by user.")

    print(f"Kafka lag CSV saved: {output}")


if __name__ == "__main__":
    main()
