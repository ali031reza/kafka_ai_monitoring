import argparse
import ast
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path


WORKER_MARKER = "[LOAD_TEST_WORKER_METRICS] "
RESULTS_MARKER = "[LOAD_TEST_RESULTS_METRICS] "
MODELS = ("car", "fire", "person_face", "ppe")


def _read_text_auto(path):
    """Read UTF-8 or Windows PowerShell UTF-16 log files safely."""
    raw = path.read_bytes()
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")

    sample = raw[:512]
    even_nuls = sample[0::2].count(0)
    odd_nuls = sample[1::2].count(0)
    if even_nuls or odd_nuls:
        encoding = "utf-16-be" if even_nuls > odd_nuls else "utf-16-le"
        try:
            return raw.decode(encoding, errors="replace")
        except UnicodeError:
            pass

    return raw.decode("utf-8", errors="replace")


def _read_csv(path):
    if not path.exists():
        return []
    text = _read_text_auto(path)
    from io import StringIO
    return list(csv.DictReader(StringIO(text)))


def _extract_marker_metrics(path, marker):
    records = []
    if not path.exists():
        return records

    for line in _read_text_auto(path).splitlines():
        position = line.find(marker)
        if position < 0:
            continue
        payload = line[position + len(marker):].strip()
        try:
            item = json.loads(payload)
            if isinstance(item, dict):
                records.append(item)
        except (ValueError, TypeError):
            continue
    return records


def _latest_worker_metrics(records):
    latest = {}
    for item in records:
        model = item.get("model")
        pid = item.get("pid")
        if model is None or pid is None:
            continue
        latest[(str(model), str(pid))] = item
    return latest


def _latest_by_pid(records):
    latest = {}
    for item in records:
        pid = item.get("pid")
        if pid is not None:
            latest[str(pid)] = item
    return list(latest.values())


def _percentile(values, percentile):
    values = sorted(float(value) for value in values if value is not None)
    if not values:
        return None
    index = max(0, math.ceil((percentile / 100.0) * len(values)) - 1)
    return values[index]


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _time_span_seconds(rows):
    timestamps = []
    for row in rows:
        value = row.get("timestamp_utc")
        if not value:
            continue
        try:
            timestamps.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            continue
    if len(timestamps) < 2:
        return 0.0
    return max(0.0, (max(timestamps) - min(timestamps)).total_seconds())


def _load_config(root):
    try:
        import yaml
        return yaml.safe_load(
            (root / "config" / "config.yaml").read_text(encoding="utf-8")
        ) or {}
    except Exception:
        return {}


def _parse_camera_log(path, config):
    result = {
        "selected_camera_count": None,
        "target_fps_per_camera": None,
        "target_source_fps": None,
        "camera_routes": {},
        "camera_summaries": {},
        "source_fps_warnings": [],
    }
    if not path.exists():
        return result

    text = _read_text_auto(path)

    selected = re.search(r"Selected cameras:\s*(\d+)", text)
    target_fps = re.search(r"Output FPS per camera:\s*([0-9.]+)", text)
    target_total = re.search(r"Aggregate target FPS:\s*([0-9.]+)", text)
    if selected:
        result["selected_camera_count"] = int(selected.group(1))
    if target_fps:
        result["target_fps_per_camera"] = float(target_fps.group(1))
    if target_total:
        result["target_source_fps"] = float(target_total.group(1))

    topic_to_model = {
        settings.get("frames"): model
        for model, settings in config.get("kafka", {}).get("topics", {}).items()
        if settings.get("frames")
    }

    for line in text.splitlines():
        match = re.match(r"^\s*(C[0-9A-Za-z_-]+):\s*(\(.*\))\s*$", line)
        if not match:
            continue
        camera_id, raw_topics = match.groups()
        try:
            topics = ast.literal_eval(raw_topics)
        except (ValueError, SyntaxError):
            continue
        if not isinstance(topics, (tuple, list)):
            continue
        result["camera_routes"][camera_id] = [
            topic_to_model[topic]
            for topic in topics
            if topic in topic_to_model
        ]

    summary_pattern = re.compile(
        r"CAMERA\s+([A-Za-z0-9_-]+)\s+SUMMARY.*?"
        r"Published frames:\s*(\d+).*?"
        r"Kafka messages queued:\s*(\d+)",
        re.DOTALL,
    )
    for match in summary_pattern.finditer(text):
        camera_id, frames, queued = match.groups()
        result["camera_summaries"][camera_id] = {
            "published_frames": int(frames),
            "queued_messages": int(queued),
        }

    for match in re.finditer(
        r"Source FPS \(([0-9.]+)\) is lower than requested FPS \(([0-9.]+)\)",
        text,
    ):
        result["source_fps_warnings"].append({
            "source_fps": float(match.group(1)),
            "requested_fps": float(match.group(2)),
        })

    return result


def _summarize_lag(rows):
    samples = defaultdict(dict)

    for row in rows:
        group_id = row.get("group_id", "")
        stream = row.get("stream_type", "")
        model = row.get("model", "all") if stream == "frames" else "all"
        timestamp = row.get("timestamp_utc", "")
        if not timestamp:
            continue

        key = (group_id, stream, model)
        sample = samples[key].setdefault(
            timestamp,
            {"known_lag": 0, "known_partitions": 0, "unknown_partitions": 0},
        )

        lag = row.get("lag", "")
        low = _safe_int(row.get("low_offset"), 0)
        committed = _safe_int(row.get("committed_offset"), -1)

        if lag not in ("", None) and committed >= low:
            try:
                sample["known_lag"] += int(lag)
                sample["known_partitions"] += 1
            except ValueError:
                sample["unknown_partitions"] += 1
        else:
            sample["unknown_partitions"] += 1

    summaries = {}
    for key, group_samples in samples.items():
        ordered = [
            (timestamp, group_samples[timestamp])
            for timestamp in sorted(group_samples)
        ]
        known = [
            (timestamp, item)
            for timestamp, item in ordered
            if item["known_partitions"] > 0
        ]
        first = known[0][1]["known_lag"] if known else None
        last = known[-1][1]["known_lag"] if known else None
        maximum = max(
            (item["known_lag"] for _, item in known),
            default=None,
        )
        summaries[key] = {
            "samples": len(ordered),
            "first_known_lag": first,
            "last_known_lag": last,
            "max_known_lag": maximum,
            "lag_delta": (last - first) if first is not None and last is not None else None,
            "max_unknown_partitions": max(
                (item["unknown_partitions"] for _, item in ordered),
                default=0,
            ),
        }
    return summaries


def _summarize_resources(rows):
    fields = (
        "system_cpu_percent",
        "system_memory_percent",
        "worker_pool_cpu_percent",
        "worker_pool_rss_mb",
        "results_processor_cpu_percent",
        "results_processor_rss_mb",
        "camera_simulator_cpu_percent",
        "camera_simulator_rss_mb",
    )
    result = {"samples": len(rows), "metrics": {}}

    for field in fields:
        values = [
            _safe_float(row.get(field))
            for row in rows
            if row.get(field) not in ("", None)
        ]
        values = [value for value in values if value is not None]
        if values:
            result["metrics"][field] = {
                "max": round(max(values), 2),
                "p95": round(_percentile(values, 95), 2),
            }

    for group in ("worker_pool", "results_processor", "camera_simulator"):
        counts = [
            _safe_int(row.get(f"{group}_process_count"), 0)
            for row in rows
            if row.get(f"{group}_process_count") not in ("", None)
        ]
        if counts:
            result["metrics"][f"{group}_process_count"] = {
                "max": max(counts),
            }

    return result


def _summarize_docker(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row.get("container", "unknown")].append(row)

    result = {}
    for container, items in groups.items():
        cpu = [_safe_float(row.get("cpu_percent")) for row in items]
        mem = [_safe_float(row.get("memory_percent")) for row in items]
        mem_mb = [_safe_float(row.get("memory_usage_mb")) for row in items]
        cpu = [value for value in cpu if value is not None]
        mem = [value for value in mem if value is not None]
        mem_mb = [value for value in mem_mb if value is not None]
        result[container] = {
            "samples": len(items),
            "max_cpu_percent": round(max(cpu), 2) if cpu else None,
            "max_memory_percent": round(max(mem), 2) if mem else None,
            "max_memory_usage_mb": round(max(mem_mb), 2) if mem_mb else None,
        }
    return result


def _weighted_stage_average(items, stage):
    count = sum(_safe_int(item.get(f"{stage}_count"), 0) for item in items)
    total = sum(_safe_float(item.get(f"{stage}_ms_total")) or 0.0 for item in items)
    return total / count if count else None


def _worst_worker_p95(items, field):
    values = [
        _safe_float(item.get(field))
        for item in items
        if item.get(field) not in (None, "")
    ]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _format(value, digits=2, suffix=""):
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{suffix}"


def _write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path, records):
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for item in records:
            handle.write(json.dumps(item, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Summarize Kafka AI load-test logs, metrics and lag"
    )
    parser.add_argument(
        "--output",
        default="artifacts/load_test/LOAD_TEST_REPORT.md",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    output_dir = root / "artifacts" / "load_test"
    output_dir.mkdir(parents=True, exist_ok=True)
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)

    config = _load_config(root)
    worker_log = output_dir / "worker_pool.log"
    results_log = output_dir / "results_processor.log"
    camera_log = output_dir / "camera_simulator.log"

    worker_records = _extract_marker_metrics(worker_log, WORKER_MARKER)
    results_records = _extract_marker_metrics(results_log, RESULTS_MARKER)

    # If a console log was not retained, allow a previously exported JSONL
    # file to serve as the source for report regeneration.
    worker_jsonl = output_dir / "worker_metrics.jsonl"
    results_jsonl = output_dir / "results_processor_metrics.jsonl"
    if not worker_records and worker_jsonl.exists():
        worker_records = [
            json.loads(line)
            for line in _read_text_auto(worker_jsonl).splitlines()
            if line.strip()
        ]
    if not results_records and results_jsonl.exists():
        results_records = [
            json.loads(line)
            for line in _read_text_auto(results_jsonl).splitlines()
            if line.strip()
        ]

    workers = _latest_worker_metrics(worker_records)
    results = _latest_by_pid(results_records)
    results = max(
        results,
        key=lambda item: _safe_float(item.get("elapsed_seconds")) or 0.0,
        default=None,
    )

    # Export normalized UTF-8 JSONL so later analysis does not need to parse
    # terminal logs or guess whether PowerShell wrote UTF-16.
    _write_jsonl(worker_jsonl, list(workers.values()))
    _write_jsonl(results_jsonl, results_records)

    lag_rows = _read_csv(output_dir / "kafka_lag.csv")
    resource_rows = _read_csv(output_dir / "resources.csv")
    docker_rows = _read_csv(output_dir / "docker_resources.csv")
    lag_summary = _summarize_lag(lag_rows)
    resource_summary = _summarize_resources(resource_rows)
    docker_summary = _summarize_docker(docker_rows)
    camera = _parse_camera_log(camera_log, config)

    lag_window = _time_span_seconds(lag_rows)
    resource_window = _time_span_seconds(resource_rows)
    test_window = max(lag_window, resource_window)
    if test_window <= 0:
        test_window = None

    camera_summaries = camera["camera_summaries"]
    total_camera_frames = sum(
        item["published_frames"] for item in camera_summaries.values()
    )
    total_routed_messages = sum(
        item["queued_messages"] for item in camera_summaries.values()
    )
    camera_observed_fps = (
        total_camera_frames / test_window
        if test_window and camera_summaries else None
    )
    routed_observed_fps = (
        total_routed_messages / test_window
        if test_window and camera_summaries else None
    )

    model_camera_ids = defaultdict(list)
    for camera_id, models in camera["camera_routes"].items():
        for model in models:
            model_camera_ids[model].append(camera_id)

    model_offered_fps = {}
    model_assignment_counts = {}
    for model in MODELS:
        camera_ids = model_camera_ids.get(model, [])
        model_assignment_counts[model] = len(camera_ids)
        if test_window and camera_summaries and camera_ids:
            model_frames = sum(
                camera_summaries.get(camera_id, {}).get("published_frames", 0)
                for camera_id in camera_ids
            )
            model_offered_fps[model] = model_frames / test_window
        elif camera.get("target_fps_per_camera") is not None and camera_ids:
            model_offered_fps[model] = (
                len(camera_ids) * camera["target_fps_per_camera"]
            )
        else:
            model_offered_fps[model] = None

    worker_groups = defaultdict(list)
    for item in workers.values():
        worker_groups[item.get("model", "unknown")].append(item)

    configured_workers = config.get("workers", {})
    worker_window = test_window or max(
        (
            _safe_float(item.get("elapsed_seconds")) or 0.0
            for item in workers.values()
        ),
        default=0.0,
    )

    lines = [
        "# Kafka AI Monitoring - Phase H Capacity Report",
        "",
        "Generated from the local benchmark logs and CSV files.",
        "",
        "## 1. Test workload and camera ingestion",
        "",
        f"- Camera threads observed in final summaries: **{len(camera_summaries)}**",
        f"- Camera threads selected at startup: **{camera.get('selected_camera_count') if camera.get('selected_camera_count') is not None else 'unknown'}**",
        f"- Requested FPS per camera: **{_format(camera.get('target_fps_per_camera'), 2)}**",
        f"- Camera frames published: **{total_camera_frames}**",
        f"- Kafka model-topic messages queued: **{total_routed_messages}**",
        f"- Estimated source FPS over monitoring window: **{_format(camera_observed_fps)}**",
        f"- Estimated routed messages/sec over monitoring window: **{_format(routed_observed_fps)}**",
        f"- Lag monitoring window: **{_format(lag_window, 1, ' s')}**",
        f"- Resource monitoring window: **{_format(resource_window, 1, ' s')}**",
        "",
        "Per-model offered load is derived from the camera-to-topic routes printed by the simulator at startup and the per-camera published-frame totals. It is not inferred from historical inference-result counts.",
        "",
        "| Model | Active camera assignments | Measured offered FPS | Configured workers | Observed worker processes |",
        "|---|---:|---:|---:|---:|",
    ]

    for model in MODELS:
        lines.append(
            f"| {model} | {model_assignment_counts.get(model, 0)} | "
            f"{_format(model_offered_fps.get(model))} | "
            f"{configured_workers.get(model, 'n/a')} | "
            f"{len(worker_groups.get(model, []))} |"
        )

    lines.extend([
        "",
        "## 2. AI worker throughput",
        "",
        "| Model | Workers observed | Processed | Skipped | Failed | Processed FPS | Offered FPS | Processed / offered | Avg JPEG ms | JPEG P95 (worst worker) | Avg inference ms | Inference P95 (worst worker) | Avg publish ms | Message P95 (worst worker) |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])

    total_worker_processed = 0
    total_worker_skipped = 0
    total_worker_failed = 0

    for model in MODELS:
        items = worker_groups.get(model, [])
        processed = sum(_safe_int(item.get("processed")) for item in items)
        skipped = sum(_safe_int(item.get("skipped")) for item in items)
        failed = sum(_safe_int(item.get("failed")) for item in items)
        total_worker_processed += processed
        total_worker_skipped += skipped
        total_worker_failed += failed

        processed_fps = (
            processed / worker_window
            if worker_window and items else None
        )
        offered_fps = model_offered_fps.get(model)
        ratio = (
            processed_fps / offered_fps * 100.0
            if processed_fps is not None and offered_fps else None
        )
        jpeg_avg = _weighted_stage_average(items, "jpeg_decode")
        inference_avg = _weighted_stage_average(items, "inference")
        publish_avg = _weighted_stage_average(items, "result_publish")
        jpeg_p95 = _worst_worker_p95(items, "jpeg_decode_p95_ms")
        inference_p95 = _worst_worker_p95(items, "inference_p95_ms")
        message_p95 = _worst_worker_p95(items, "total_message_p95_ms")

        lines.append(
            f"| {model} | {len(items)} | {processed} | {skipped} | {failed} | "
            f"{_format(processed_fps)} | {_format(offered_fps)} | "
            f"{_format(ratio, 1, '%')} | {_format(jpeg_avg)} | "
            f"{_format(jpeg_p95)} | {_format(inference_avg)} | "
            f"{_format(inference_p95)} | {_format(publish_avg)} | "
            f"{_format(message_p95)} |"
        )

    lines.extend([
        "",
        f"- Total processed worker messages: **{total_worker_processed}**",
        f"- Total skipped worker messages: **{total_worker_skipped}**",
        f"- Total failed worker messages: **{total_worker_failed}**",
        f"- Worker metric snapshots parsed: **{len(worker_records)}**",
        f"- Distinct worker processes observed: **{len(workers)} / {sum(_safe_int(value) for value in configured_workers.values()) if configured_workers else 'unknown'} configured**",
        "",
        "Processed FPS uses the measured camera/monitoring window where available. Stage P95 values are the highest per-worker P95 values from bounded worker samples; they are not a merged percentile across all workers.",
        "",
        "## 3. Kafka consumer lag trend",
        "",
        "| Consumer group | Stream | Model | First known lag | Last known lag | Maximum known lag | Change (last - first) | Max unknown partitions | Samples |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ])

    for key in sorted(lag_summary):
        group_id, stream, model = key
        item = lag_summary[key]
        lines.append(
            f"| {group_id} | {stream} | {model} | "
            f"{item['first_known_lag'] if item['first_known_lag'] is not None else 'unknown'} | "
            f"{item['last_known_lag'] if item['last_known_lag'] is not None else 'unknown'} | "
            f"{item['max_known_lag'] if item['max_known_lag'] is not None else 'unknown'} | "
            f"{item['lag_delta'] if item['lag_delta'] is not None else 'unknown'} | "
            f"{item['max_unknown_partitions']} | {item['samples']} |"
        )

    if not lag_summary:
        lines.append("No Kafka lag samples found.")

    lines.extend([
        "",
        "A positive lag change means the final known lag exceeded the first known lag. Unknown partitions are not treated as zero. Compare the last live sample with the post-stop drain sample separately.",
        "",
        "## 4. Results processor and PostgreSQL write path",
        "",
    ])

    if results:
        result_processed = _safe_int(results.get("processed"))
        result_failed = _safe_int(results.get("failed"))
        result_window_fps = (
            result_processed / test_window
            if test_window else None
        )
        result_service_fps = (
            result_processed / (_safe_float(results.get("elapsed_seconds")) or 1.0)
        )
        lines.extend([
            f"- Results processed: **{result_processed}**",
            f"- Results failed: **{result_failed}**",
            f"- Results processor lifetime throughput: **{_format(result_service_fps)} results/s**",
            f"- Results persisted per camera monitoring second: **{_format(result_window_fps)} results/s**",
            f"- Batches: **{results.get('batches', 'n/a')}**",
            f"- Average batch size: **{results.get('batch_size_avg', 'n/a')}**",
            f"- Average DB transaction per batch: **{results.get('db_transaction_avg_ms', 'n/a')} ms**",
            f"- Average DB transaction per message: **{results.get('db_transaction_avg_ms_per_message', 'n/a')} ms**",
            f"- Average Kafka offset commit per batch: **{results.get('offset_commit_avg_ms', 'n/a')} ms**",
            f"- Results processor metric snapshots parsed: **{len(results_records)}**",
        ])
    else:
        lines.append("No results-processor metrics were parsed from results_processor.log.")

    lines.extend([
        "",
        "## 5. System, worker, simulator and Docker resources",
        "",
        "| Metric | Maximum observed | P95 observed |",
        "|---|---:|---:|",
    ])

    resource_labels = (
        ("system_cpu_percent", "System CPU"),
        ("system_memory_percent", "System RAM utilization"),
        ("worker_pool_cpu_percent", "Worker process-tree CPU (one-core %)"),
        ("worker_pool_rss_mb", "Worker process-tree RAM"),
        ("results_processor_cpu_percent", "Results processor CPU (one-core %)"),
        ("results_processor_rss_mb", "Results processor RAM"),
        ("camera_simulator_cpu_percent", "Camera simulator CPU (one-core %)"),
        ("camera_simulator_rss_mb", "Camera simulator RAM"),
    )

    for field, label in resource_labels:
        metric = resource_summary.get("metrics", {}).get(field)
        if metric:
            suffix = "%" if field.endswith("percent") else " MB"
            lines.append(
                f"| {label} | {_format(metric['max'], 2, suffix)} | "
                f"{_format(metric.get('p95'), 2, suffix)} |"
            )

    if not resource_rows:
        lines.append("| Resource monitor | No CSV found | n/a |")

    lines.extend([
        "",
        f"- Resource samples: **{resource_summary.get('samples', 0)}**",
        f"- Maximum worker process count (including pool parent): **{resource_summary.get('metrics', {}).get('worker_pool_process_count', {}).get('max', 'unknown')}**",
        f"- Maximum results processor process count: **{resource_summary.get('metrics', {}).get('results_processor_process_count', {}).get('max', 'unknown')}**",
        "",
        "Process-tree CPU is expressed as a percentage of one logical CPU and can exceed 100% when multiple cores are used. System CPU is the host-wide percentage.",
        "",
        "### Docker containers",
        "",
    ])

    if docker_summary:
        lines.extend([
            "| Container | Maximum CPU | Maximum memory | Maximum memory utilization | Samples |",
            "|---|---:|---:|---:|---:|",
        ])
        for container, item in sorted(docker_summary.items()):
            lines.append(
                f"| {container} | {_format(item['max_cpu_percent'], 2, '%')} | "
                f"{_format(item['max_memory_usage_mb'], 2, ' MiB')} | "
                f"{_format(item['max_memory_percent'], 2, '%')} | {item['samples']} |"
            )
    else:
        lines.append("No Docker stats CSV found. Rerun resource_monitor with --docker-stats to attribute container CPU/RAM.")

    lines.extend([
        "",
        "## 6. PostgreSQL metadata benchmark",
        "",
    ])

    postgres_path = output_dir / "postgres_benchmark.json"
    if postgres_path.exists():
        try:
            postgres = json.loads(_read_text_auto(postgres_path))
            lines.extend([
                f"- Mode: **{postgres.get('mode', 'n/a')}**",
                f"- Completed rows: **{postgres.get('completed_rows', 'n/a')} / {postgres.get('requested_rows', 'n/a')}**",
                f"- Throughput: **{postgres.get('rows_per_second', 'n/a')} rows/s**",
                f"- Average transaction: **{postgres.get('transaction_avg_ms', 'n/a')} ms**",
                f"- P95 transaction: **{postgres.get('transaction_p95_ms', 'n/a')} ms**",
                f"- Durable commit measured: **{postgres.get('durable_commit_measured', 'n/a')}**",
                f"- Cleanup completed: **{postgres.get('cleanup_completed', 'n/a')}**",
            ])
        except (ValueError, OSError) as error:
            lines.append(f"Could not parse PostgreSQL benchmark JSON: {error}")
    else:
        lines.append("No PostgreSQL benchmark JSON found. Run the isolated PostgreSQL benchmark after the live test.")

    warnings = []
    expected_total = sum(
        _safe_int(value) for value in configured_workers.values()
    ) if configured_workers else None
    if expected_total is not None and len(workers) < expected_total:
        warnings.append(
            f"Only {len(workers)} worker processes emitted metrics; {expected_total} were configured."
        )
    if camera.get("selected_camera_count") and len(camera_summaries) < camera["selected_camera_count"]:
        warnings.append(
            f"Only {len(camera_summaries)} camera summaries were found for {camera['selected_camera_count']} selected cameras."
        )
    system_cpu = resource_summary.get("metrics", {}).get("system_cpu_percent", {}).get("max")
    system_ram = resource_summary.get("metrics", {}).get("system_memory_percent", {}).get("max")
    if system_cpu is not None and system_cpu >= 95:
        warnings.append(f"System CPU reached {system_cpu:.1f}%; host CPU saturation is likely.")
    if system_ram is not None and system_ram >= 95:
        warnings.append(f"System RAM reached {system_ram:.1f}%; memory headroom was critically low.")
    for key, item in lag_summary.items():
        if item["lag_delta"] is not None and item["lag_delta"] > 0:
            warnings.append(
                f"Kafka lag increased for {key[0]} / {key[1]} / {key[2]} by {item['lag_delta']} messages."
            )
    if total_worker_failed:
        warnings.append(f"AI workers reported {total_worker_failed} failed messages.")
    if total_worker_skipped:
        warnings.append(
            f"AI workers skipped {total_worker_skipped} messages; distinguish freshness skips from other skip causes in worker logs."
        )
    if results and _safe_int(results.get("failed")):
        warnings.append(
            f"Results processor reported {_safe_int(results.get('failed'))} failed messages."
        )

    lines.extend([
        "",
        "## 7. Capacity warnings and interpretation",
        "",
    ])
    if warnings:
        lines.extend(f"- **WARNING:** {warning}" for warning in warnings)
    else:
        lines.append("No automatic warning conditions were detected. This is not by itself a production capacity sign-off.")

    lines.extend([
        "",
        "## 8. Measurement limitations",
        "",
        "- Worker and results-processor counts are derived from their final cumulative metric snapshots. The normalized JSONL files are exported as UTF-8 by this summarizer.",
        "- The worker throughput comparison uses the measured camera/monitoring window. It should be interpreted together with Kafka lag and the post-stop drain test.",
        "- The camera assignment matrix is parsed from the simulator's startup routing display. If that section is missing from the camera log, per-model offered FPS is unavailable rather than inferred from historical database results.",
        "- Docker container metrics are available only when resource_monitor.py is run with --docker-stats.",
        "- The PostgreSQL benchmark is separate from the live pipeline and should be run after the live test has stopped.",
        "",
    ])

    report = "\n".join(lines)
    output.write_text(report, encoding="utf-8")
    _write_json(output_dir / "kafka_lag_summary.json", {
        "|".join(key): value for key, value in lag_summary.items()
    })
    _write_json(output_dir / "resource_summary.json", resource_summary)
    _write_json(output_dir / "camera_summary.json", {
        "test_window_seconds": test_window,
        "selected_camera_count": camera.get("selected_camera_count"),
        "camera_summaries": camera_summaries,
        "camera_routes": camera["camera_routes"],
        "model_assignment_counts": model_assignment_counts,
        "model_offered_fps": model_offered_fps,
        "total_published_frames": total_camera_frames,
        "total_routed_messages_queued": total_routed_messages,
        "estimated_source_fps": camera_observed_fps,
        "estimated_routed_messages_fps": routed_observed_fps,
    })
    _write_json(output_dir / "docker_resource_summary.json", docker_summary)

    print(f"Load test report written: {output}")
    print(f"Worker metrics JSONL: {worker_jsonl}")
    print(f"Results processor metrics JSONL: {results_jsonl}")
    print(f"Camera summary JSON: {output_dir / 'camera_summary.json'}")
    print(f"Kafka lag summary JSON: {output_dir / 'kafka_lag_summary.json'}")
    print(f"Resource summary JSON: {output_dir / 'resource_summary.json'}")


if __name__ == "__main__":
    main()
