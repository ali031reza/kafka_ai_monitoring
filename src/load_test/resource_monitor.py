import argparse
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil


def _matching_roots():
    roots = {
        "worker_pool": [],
        "results_processor": [],
    }

    for process in psutil.process_iter(
        attrs=["pid", "cmdline"]
    ):
        try:
            if process.info["pid"] == os.getpid():
                continue
            command = " ".join(process.info.get("cmdline") or [])
            if "src.run_worker_pool" in command:
                roots["worker_pool"].append(process)
            if "src.run_results_processor" in command:
                roots["results_processor"].append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return roots


def _process_group_stats(root_processes):
    process_by_pid = {}

    for root in root_processes:
        try:
            process_by_pid[root.pid] = root
            for child in root.children(recursive=True):
                process_by_pid[child.pid] = child
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    cpu_percent = 0.0
    rss_bytes = 0
    active_count = 0

    for process in process_by_pid.values():
        try:
            cpu_percent += process.cpu_percent(interval=None)
            rss_bytes += process.memory_info().rss
            active_count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return {
        "process_count": active_count,
        "cpu_percent": round(cpu_percent, 2),
        "rss_mb": round(rss_bytes / (1024 * 1024), 2),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Monitor system and Kafka AI Python process CPU/RAM"
    )
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument(
        "--output",
        default="artifacts/load_test/resources.csv",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.duration <= 0:
        parser.error("--duration must be positive")

    root = Path(__file__).resolve().parents[2]
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    output.parent.mkdir(parents=True, exist_ok=True)

    # Prime psutil CPU counters so the first sample is meaningful.
    psutil.cpu_percent(interval=None)
    for process in psutil.process_iter():
        try:
            process.cpu_percent(interval=None)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    columns = [
        "timestamp_utc",
        "system_cpu_percent",
        "system_memory_percent",
        "system_memory_used_mb",
        "worker_pool_process_count",
        "worker_pool_cpu_percent",
        "worker_pool_rss_mb",
        "results_processor_process_count",
        "results_processor_cpu_percent",
        "results_processor_rss_mb",
    ]

    started = time.monotonic()
    sample_number = 0

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()

        try:
            while time.monotonic() - started < args.duration:
                time.sleep(args.interval)

                roots = _matching_roots()
                worker_stats = _process_group_stats(
                    roots["worker_pool"]
                )
                results_stats = _process_group_stats(
                    roots["results_processor"]
                )
                memory = psutil.virtual_memory()

                row = {
                    "timestamp_utc": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "system_cpu_percent": psutil.cpu_percent(
                        interval=None
                    ),
                    "system_memory_percent": memory.percent,
                    "system_memory_used_mb": round(
                        memory.used / (1024 * 1024),
                        2,
                    ),
                    "worker_pool_process_count": worker_stats[
                        "process_count"
                    ],
                    "worker_pool_cpu_percent": worker_stats[
                        "cpu_percent"
                    ],
                    "worker_pool_rss_mb": worker_stats["rss_mb"],
                    "results_processor_process_count": results_stats[
                        "process_count"
                    ],
                    "results_processor_cpu_percent": results_stats[
                        "cpu_percent"
                    ],
                    "results_processor_rss_mb": results_stats["rss_mb"],
                }
                writer.writerow(row)
                handle.flush()

                sample_number += 1
                if sample_number % 5 == 0:
                    print(
                        "[RESOURCE] "
                        f"system_cpu={row['system_cpu_percent']:.1f}% "
                        f"system_ram={row['system_memory_percent']:.1f}% "
                        f"workers_cpu={row['worker_pool_cpu_percent']:.1f}% "
                        f"workers_ram={row['worker_pool_rss_mb']:.0f}MB "
                        f"results_cpu={row['results_processor_cpu_percent']:.1f}% "
                        f"results_ram={row['results_processor_rss_mb']:.0f}MB",
                        flush=True,
                    )
        except KeyboardInterrupt:
            print("\nResource monitoring stopped by user.")

    print(f"Resource CSV saved: {output}")


if __name__ == "__main__":
    main()
