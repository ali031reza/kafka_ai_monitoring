import argparse
import csv
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil


PROCESS_GROUPS = (
    "worker_pool",
    "results_processor",
    "camera_simulator",
)


def _matching_roots():
    roots = {name: [] for name in PROCESS_GROUPS}

    for process in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            if process.info["pid"] == os.getpid():
                continue

            command = " ".join(process.info.get("cmdline") or [])
            if "src.run_worker_pool" in command:
                roots["worker_pool"].append(process)
            if "src.run_results_processor" in command:
                roots["results_processor"].append(process)
            if "src.run_multi_camera_simulator" in command:
                roots["camera_simulator"].append(process)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return roots


def _process_group_stats(root_processes, cpu_state, sampled_at):
    """
    Measure a process tree using CPU-time deltas.

    psutil.Process.cpu_percent() is stateful. Recreating Process objects for
    every sample resets its baseline and commonly produces zero readings.
    This implementation keeps the previous CPU times by PID and process
    creation time, so each sample measures real CPU use since the prior one.

    CPU percentages are expressed as one-core percentages. A process tree
    using two full logical cores can therefore report approximately 200%.
    """
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
    current_pids = set()

    for pid, process in process_by_pid.items():
        try:
            created_at = process.create_time()
            cpu_times = process.cpu_times()
            total_cpu_seconds = cpu_times.user + cpu_times.system
            rss_bytes += process.memory_info().rss
            active_count += 1
            current_pids.add(pid)

            previous = cpu_state.get(pid)
            if previous is not None:
                previous_created_at, previous_cpu, previous_sampled_at = previous
                elapsed = sampled_at - previous_sampled_at

                # A different creation time means Windows reused the PID.
                if previous_created_at == created_at and elapsed > 0:
                    cpu_delta = max(0.0, total_cpu_seconds - previous_cpu)
                    cpu_percent += (cpu_delta / elapsed) * 100.0

            cpu_state[pid] = (
                created_at,
                total_cpu_seconds,
                sampled_at,
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    # Drop exited processes so the cache remains bounded.
    for stale_pid in set(cpu_state) - current_pids:
        cpu_state.pop(stale_pid, None)

    return {
        "process_count": active_count,
        "cpu_percent": round(cpu_percent, 2),
        "rss_mb": round(rss_bytes / (1024 * 1024), 2),
    }


def _memory_to_mb(value):
    """Convert Docker's human-readable memory quantity to MiB."""
    match = re.match(
        r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)",
        value or "",
    )
    if not match:
        return None

    amount = float(match.group(1))
    unit = match.group(2).lower()
    factors = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000 ** 2,
        "mib": 1024 ** 2,
        "gb": 1000 ** 3,
        "gib": 1024 ** 3,
        "tb": 1000 ** 4,
        "tib": 1024 ** 4,
    }
    factor = factors.get(unit)
    if factor is None:
        return None
    return round((amount * factor) / (1024 ** 2), 2)


def _docker_stats():
    """Return one Docker stats row per running container."""
    completed = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or "docker stats failed"
        raise RuntimeError(message)

    rows = []
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue

        item = json.loads(line)
        memory_usage, _, memory_limit = item.get("MemUsage", "").partition("/")
        rows.append({
            "container": item.get("Name") or item.get("Container") or item.get("ID", "unknown"),
            "cpu_percent": _parse_percent(item.get("CPUPerc")),
            "memory_usage_mb": _memory_to_mb(memory_usage),
            "memory_limit_mb": _memory_to_mb(memory_limit),
            "memory_percent": _parse_percent(item.get("MemPerc")),
            "pids": item.get("PIDs", ""),
            "memory_usage_raw": item.get("MemUsage", ""),
        })
    return rows


def _parse_percent(value):
    if value in (None, ""):
        return None
    try:
        return float(str(value).strip().rstrip("%"))
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Measure system, Python process-tree and optional Docker "
            "CPU/RAM during a Kafka AI load test"
        )
    )
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=300.0)
    parser.add_argument(
        "--output",
        default="artifacts/load_test/resources.csv",
    )
    parser.add_argument(
        "--docker-stats",
        action="store_true",
        help="Also sample CPU/RAM for running Docker containers",
    )
    parser.add_argument(
        "--docker-interval",
        type=float,
        default=5.0,
        help="Seconds between Docker stats samples (default: 5)",
    )
    parser.add_argument(
        "--docker-output",
        default="artifacts/load_test/docker_resources.csv",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.docker_interval <= 0:
        parser.error("--docker-interval must be positive")

    root = Path(__file__).resolve().parents[2]

    def resolve_output(value):
        path = Path(value)
        if not path.is_absolute():
            path = root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    output = resolve_output(args.output)
    docker_output = resolve_output(args.docker_output)

    columns = [
        "timestamp_utc",
        "system_cpu_percent",
        "system_memory_percent",
        "system_memory_used_mb",
        "system_memory_total_mb",
    ]
    for group in PROCESS_GROUPS:
        columns.extend([
            f"{group}_process_count",
            f"{group}_cpu_percent",
            f"{group}_rss_mb",
        ])

    cpu_state = {name: {} for name in PROCESS_GROUPS}
    psutil.cpu_percent(interval=None)  # Prime the system CPU counter.

    docker_handle = None
    docker_writer = None
    if args.docker_stats:
        docker_columns = [
            "timestamp_utc",
            "container",
            "cpu_percent",
            "memory_usage_mb",
            "memory_limit_mb",
            "memory_percent",
            "pids",
            "memory_usage_raw",
        ]
        docker_handle = docker_output.open(
            "w",
            newline="",
            encoding="utf-8",
        )
        docker_writer = csv.DictWriter(
            docker_handle,
            fieldnames=docker_columns,
        )
        docker_writer.writeheader()

    started = time.monotonic()
    next_docker_sample = started
    sample_number = 0

    print(
        f"[RESOURCE MONITOR] interval={args.interval}s, "
        f"duration={args.duration}s, docker_stats={args.docker_stats}",
        flush=True,
    )

    try:
        with output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()

            try:
                while time.monotonic() - started < args.duration:
                    remaining = args.duration - (time.monotonic() - started)
                    time.sleep(min(args.interval, max(remaining, 0.0)))

                    sampled_at = time.monotonic()
                    timestamp = datetime.now(timezone.utc).isoformat()
                    roots = _matching_roots()
                    memory = psutil.virtual_memory()

                    row = {
                        "timestamp_utc": timestamp,
                        "system_cpu_percent": psutil.cpu_percent(interval=None),
                        "system_memory_percent": memory.percent,
                        "system_memory_used_mb": round(
                            memory.used / (1024 * 1024), 2
                        ),
                        "system_memory_total_mb": round(
                            memory.total / (1024 * 1024), 2
                        ),
                    }

                    for group in PROCESS_GROUPS:
                        stats = _process_group_stats(
                            roots[group],
                            cpu_state[group],
                            sampled_at,
                        )
                        row[f"{group}_process_count"] = stats["process_count"]
                        row[f"{group}_cpu_percent"] = stats["cpu_percent"]
                        row[f"{group}_rss_mb"] = stats["rss_mb"]

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
                            f"results_ram={row['results_processor_rss_mb']:.0f}MB "
                            f"camera_cpu={row['camera_simulator_cpu_percent']:.1f}%",
                            flush=True,
                        )

                    if (
                        docker_writer is not None
                        and sampled_at >= next_docker_sample
                    ):
                        try:
                            for docker_row in _docker_stats():
                                docker_writer.writerow({
                                    "timestamp_utc": timestamp,
                                    **docker_row,
                                })
                            docker_handle.flush()
                        except (
                            OSError,
                            subprocess.SubprocessError,
                            json.JSONDecodeError,
                            RuntimeError,
                        ) as error:
                            print(
                                f"[DOCKER_STATS_WARNING] {error}",
                                flush=True,
                            )
                        next_docker_sample = (
                            sampled_at + args.docker_interval
                        )

            except KeyboardInterrupt:
                print("\nResource monitoring stopped by user.")

    finally:
        if docker_handle is not None:
            docker_handle.close()

    print(f"Resource CSV saved: {output}")
    if args.docker_stats:
        print(f"Docker resource CSV saved: {docker_output}")


if __name__ == "__main__":
    main()
