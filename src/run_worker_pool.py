import argparse
import multiprocessing as mp
import queue
import time
from pathlib import Path

from config.config_manager import ConfigManager
from src.run_worker import WorkerApplication


def _worker_process_main(model_name, worker_number, stop_event, error_queue):
    """Child process entry point; each process owns one Kafka consumer."""
    worker_id = f"{model_name}-{worker_number:02d}"
    try:
        app = WorkerApplication(
            model_name=model_name,
            worker_instance_id=worker_id,
        )
        app.run(stop_event=stop_event)
    except BaseException as error:
        error_queue.put(
            f"{worker_id}: {type(error).__name__}: {error}"
        )
        stop_event.set()
        raise


class WorkerPoolApplication:
    """Starts configured model replicas as separate OS processes."""

    def __init__(self, selected_models=None):
        root = Path(__file__).resolve().parents[1]
        config = ConfigManager(root)
        kafka = config.get_section("kafka")
        worker_config = config.get_section("workers")
        topic_config = kafka["topics"]

        models = selected_models or list(topic_config.keys())
        self._plan = []

        for model_name in models:
            if model_name not in topic_config:
                raise ValueError(f"Unknown model: {model_name}")

            replicas = int(worker_config.get(model_name, 1))
            partitions = int(topic_config[model_name].get("partitions", 4))

            if replicas <= 0:
                raise ValueError(f"Worker count for {model_name} must be positive")
            if replicas > partitions:
                raise ValueError(
                    f"{model_name}: {replicas} workers exceed "
                    f"{partitions} configured frame partitions"
                )

            self._plan.append((model_name, replicas))

        if not self._plan:
            raise ValueError("No worker models selected")

        self._ctx = mp.get_context("spawn")
        self._stop_event = self._ctx.Event()
        self._error_queue = self._ctx.Queue()
        self._processes = []

    def run(self):
        total = sum(count for _, count in self._plan)

        print("\n==========================================")
        print("AI WORKER PROCESS POOL")
        print("==========================================")
        for model, count in self._plan:
            print(f"{model}: {count} processes")
        print(f"Total processes: {total}")
        print("All replicas of a model share that model's consumer group.")
        print("Press CTRL+C to stop all workers.")

        try:
            for model, count in self._plan:
                for number in range(1, count + 1):
                    process = self._ctx.Process(
                        target=_worker_process_main,
                        args=(
                            model,
                            number,
                            self._stop_event,
                            self._error_queue,
                        ),
                        name=f"{model}-{number:02d}",
                    )
                    process.start()
                    self._processes.append(process)
                    time.sleep(0.05)

            while any(process.is_alive() for process in self._processes):
                if self._stop_event.is_set():
                    break

                for process in self._processes:
                    if process.exitcode not in (None, 0):
                        self._stop_event.set()
                        break

                time.sleep(0.5)

        except KeyboardInterrupt:
            print("\n[STOP REQUESTED] Stopping worker pool...")

        finally:
            self._stop_event.set()

            for process in self._processes:
                process.join(timeout=10)

            for process in self._processes:
                if process.is_alive():
                    print(f"[WARNING] Terminating {process.name}")
                    process.terminate()
                    process.join(timeout=5)

        errors = []
        while True:
            try:
                errors.append(self._error_queue.get_nowait())
            except queue.Empty:
                break

        unexpected = [
            (process.name, process.exitcode)
            for process in self._processes
            if process.exitcode not in (0, None)
        ]

        self._error_queue.close()
        print("\nWorker pool stopped.")

        if errors:
            print("Worker errors:")
            for error in errors:
                print(error)
        if unexpected:
            print("Unexpected exits:")
            for name, code in unexpected:
                print(f"{name}: exit code {code}")
        if errors or unexpected:
            raise RuntimeError("One or more AI workers failed")


class WorkerPoolCLI:
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description="Start configured Kafka AI worker replicas"
        )
        self._parser.add_argument(
            "--models",
            default=None,
            help="Optional comma-separated model list",
        )

    def run(self):
        args = self._parser.parse_args()
        selected = None
        if args.models:
            selected = [
                item.strip()
                for item in args.models.split(",")
                if item.strip()
            ]
        WorkerPoolApplication(selected_models=selected).run()


if __name__ == "__main__":
    mp.freeze_support()
    WorkerPoolCLI().run()
