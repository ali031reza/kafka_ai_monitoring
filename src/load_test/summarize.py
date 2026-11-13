import csv
import json
from collections import defaultdict
from pathlib import Path


MARKER = "[LOAD_TEST_WORKER_METRICS] "
RESULTS_MARKER = "[LOAD_TEST_RESULTS_METRICS] "


def _read_worker_metrics(path):
    latest = {}
    if not path.exists():
        return latest

    for line in path.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():
        position = line.find(MARKER)
        if position < 0:
            continue
        try:
            item = json.loads(line[position + len(MARKER):])
            latest[(item["model"], item["pid"])] = item
        except (ValueError, KeyError):
            continue
    return latest


def _read_latest_json_metric(path, marker):
    latest = None
    if not path.exists():
        return None
    for line in path.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():
        position = line.find(marker)
        if position < 0:
            continue
        try:
            latest = json.loads(line[position + len(marker):])
        except ValueError:
            continue
    return latest


def _read_csv(path):
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main():
    root = Path(__file__).resolve().parents[2]
    output_dir = root / "artifacts" / "load_test"
    output_dir.mkdir(parents=True, exist_ok=True)

    workers = _read_worker_metrics(
        output_dir / "worker_pool.log"
    )
    results = _read_latest_json_metric(
        output_dir / "results_processor.log",
        RESULTS_MARKER,
    )
    lag_rows = _read_csv(output_dir / "kafka_lag.csv")
    resource_rows = _read_csv(output_dir / "resources.csv")

    grouped = defaultdict(list)
    for item in workers.values():
        grouped[item["model"]].append(item)

    lines = [
        "# Kafka AI Monitoring — Load Test Report",
        "",
        "Generated from the local run logs and CSV files.",
        "",
        "## 1. AI worker throughput",
        "",
        "| Model | Worker processes | Processed | Skipped | Failed | Aggregate processed FPS | Avg JPEG decode ms | Avg inference ms | Avg event image save ms | Avg result publish ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for model in ("car", "fire", "person_face", "ppe"):
        items = grouped.get(model, [])
        if not items:
            lines.append(
                f"| {model} | — | — | — | — | — | — | — | — | — |"
            )
            continue

        elapsed = max(
            (item.get("active_elapsed_seconds", item.get("elapsed_seconds", 0.0)) for item in items),
            default=0.0,
        )
        processed = sum(item.get("processed", 0) for item in items)
        skipped = sum(item.get("skipped", 0) for item in items)
        failed = sum(item.get("failed", 0) for item in items)

        def weighted_avg(stage):
            count = sum(
                item.get(f"{stage}_count", 0)
                for item in items
            )
            total = sum(
                item.get(f"{stage}_ms_total", 0.0)
                for item in items
            )
            return total / count if count else 0.0

        fps = processed / elapsed if elapsed else 0.0
        lines.append(
            f"| {model} | {len(items)} | {processed} | {skipped} | "
            f"{failed} | {fps:.2f} | "
            f"{weighted_avg('jpeg_decode'):.2f} | "
            f"{weighted_avg('inference'):.2f} | "
            f"{weighted_avg('event_image_save'):.2f} | "
            f"{weighted_avg('result_publish'):.2f} |"
        )

    lines.extend([
        "",
        "Aggregate model FPS is total processed messages divided by the longest worker elapsed time for that model. Run all workers in one test window for the most meaningful comparison.",
        "",
        "## 2. Kafka consumer lag",
        "",
    ])

    if lag_rows:
        totals = defaultdict(int)
        known_partition_counts = defaultdict(int)
        unknown_partition_counts = defaultdict(int)
        for row in lag_rows:
            key = (
                row["group_id"],
                row["stream_type"],
                row["model"] if row["stream_type"] == "frames" else "all",
                row["timestamp_utc"],
            )
            if row.get("lag", "") in ("", None):
                unknown_partition_counts[key] += 1
            else:
                totals[key] += int(row["lag"])
                known_partition_counts[key] += 1

        maxima = {}
        max_unknown = defaultdict(int)
        for key, lag in totals.items():
            group_key = key[:3]
            maxima[group_key] = max(maxima.get(group_key, 0), lag)
        for key, count in unknown_partition_counts.items():
            group_key = key[:3]
            max_unknown[group_key] = max(max_unknown[group_key], count)

        group_keys = sorted(set(maxima) | set(max_unknown))
        lines.extend([
            "| Consumer group | Stream | Model | Maximum sampled known lag | Unknown partitions per sample (max) |",
            "|---|---|---|---:|---:|",
        ])
        for group_key in group_keys:
            group_id, stream_type, model = group_key
            known_lag = (
                str(maxima[group_key])
                if group_key in maxima
                else "Unknown"
            )
            lines.append(
                f"| {group_id} | {stream_type} | {model} | "
                f"{known_lag} | {max_unknown.get(group_key, 0)} |"
            )
        lines.append(
            "Known lag sums only partitions with valid committed offsets. "
            "Partitions without a valid committed offset, or whose committed "
            "offset is older than the low watermark, are reported as unknown "
            "rather than being assumed to start at the earliest offset."
        )
    else:
        lines.append("No Kafka lag CSV found.")

    lines.extend([
        "",
        "## 3. CPU and RAM",
        "",
    ])

    if resource_rows:
        def max_value(field):
            values = [
                float(row[field])
                for row in resource_rows
                if row.get(field) not in (None, "")
            ]
            return max(values) if values else 0.0

        lines.extend([
            "| Metric | Maximum observed |",
            "|---|---:|",
            f"| System CPU | {max_value('system_cpu_percent'):.2f}% |",
            f"| System RAM utilization | {max_value('system_memory_percent'):.2f}% |",
            f"| Worker pool CPU | {max_value('worker_pool_cpu_percent'):.2f}% |",
            f"| Worker pool RAM | {max_value('worker_pool_rss_mb'):.2f} MB |",
            f"| Results processor CPU | {max_value('results_processor_cpu_percent'):.2f}% |",
            f"| Results processor RAM | {max_value('results_processor_rss_mb'):.2f} MB |",
        ])
    else:
        lines.append("No resource-monitor CSV found.")

    lines.extend([
        "",
        "## 4. Results processor throughput",
        "",
    ])

    if results:
        lines.extend([
            f"- Processed results: **{results.get('processed', 0)}**",
            f"- Results throughput: **{results.get('results_fps', 0)} results/s**",
            f"- Average DB transaction: **{results.get('db_transaction_avg_ms', 0)} ms**",
            f"- Average Kafka offset commit: **{results.get('offset_commit_avg_ms', 0)} ms**",
            f"- Failed results: **{results.get('failed', 0)}**",
        ])
    else:
        lines.append("No results-processor metrics found.")

    lines.extend([
        "",
        "## 5. PostgreSQL metadata insertion benchmark",
        "",
    ])

    postgres_path = output_dir / "postgres_benchmark.json"
    if postgres_path.exists():
        postgres = json.loads(postgres_path.read_text(encoding="utf-8"))
        lines.extend([
            f"- Mode: **{postgres.get('mode')}**",
            f"- Completed rows: **{postgres.get('completed_rows')}**",
            f"- Throughput: **{postgres.get('rows_per_second')} rows/s**",
            f"- Average transaction: **{postgres.get('transaction_avg_ms')} ms**",
            f"- P95 transaction: **{postgres.get('transaction_p95_ms')} ms**",
            f"- Durable commit measured: **{postgres.get('durable_commit_measured')}**",
            f"- Cleanup completed: **{postgres.get('cleanup_completed')}**",
        ])
    else:
        lines.append("No PostgreSQL benchmark JSON found.")

    lines.extend([
        "",
        "## Interpretation notes",
        "",
        "- Worker metrics are emitted by the application. Processed FPS is measured across the worker run, not inferred from the configured delay.",
        "- Kafka lag is sampled from each model input consumer group and the shared results processor group.",
        "- CPU and RAM include the worker-pool process tree and results-processor process tree, plus whole-system utilization.",
        "- Results processor throughput includes Kafka consumption, Protobuf validation, PostgreSQL transaction and offset commit.",
        "- The PostgreSQL benchmark uses the application's repositories and measures event-image metadata plus inference-result metadata. Commit mode commits tagged test rows and removes them afterward.",
        "- Compare results only when the camera simulator, workers, results processor and monitoring windows overlap as documented.",
        "",
    ])

    report = output_dir / "LOAD_TEST_REPORT.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    print(f"Load test report written: {report}")


if __name__ == "__main__":
    main()
