import json
import os
import time

from google.protobuf.message import DecodeError

from src.proto.inference_result_pb2 import InferenceResult

from src.kafka.results_consumer import KafkaResultsConsumer

from src.database.client import DatabaseClient
from src.database.image_repository import ImageRepository
from src.database.results_repository import ResultsRepository


class ResultsProcessingService:
    """
    Processes inference results from Kafka in batches.

    Processing order:
        1. Poll a batch from Kafka.
        2. Deserialize and validate every message.
        3. Persist the complete batch in one PostgreSQL transaction.
        4. Commit the consumed Kafka offsets synchronously.

    If database persistence or the Kafka commit fails, the service stops
    without intentionally advancing the offsets. Database repositories
    provide idempotency for messages replayed after a successful database
    commit but an unsuccessful Kafka offset commit.
    """

    def __init__(
        self,
        consumer: KafkaResultsConsumer,
        database: DatabaseClient,
        image_repository: ImageRepository,
        results_repository: ResultsRepository,
        topic_model_map: dict[str, str],
        batch_size: int = 100,
        batch_timeout_ms: int = 20,
        poll_timeout_seconds: float = 1.0,
    ):
        if not topic_model_map:
            raise ValueError("No result topics configured")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if batch_timeout_ms < 0:
            raise ValueError("batch_timeout_ms cannot be negative")
        if poll_timeout_seconds < 0:
            raise ValueError("poll_timeout_seconds cannot be negative")

        self._consumer = consumer
        self._database = database
        self._image_repository = image_repository
        self._results_repository = results_repository
        self._topic_model_map = topic_model_map

        self._batch_size = int(batch_size)
        self._batch_timeout_ms = int(batch_timeout_ms)
        self._poll_timeout_seconds = float(poll_timeout_seconds)

        self._run_started_at = None
        self._first_message_at = None
        self._last_message_at = None

        self._metrics = {
            "messages": 0,
            "processed": 0,
            "failed": 0,
            "batches": 0,
            "batch_size_total": 0,
            "max_batch_size": 0,
            "db_transaction_ms_total": 0.0,
            "db_transaction_count": 0,
            "offset_commit_ms_total": 0.0,
            "offset_commit_count": 0,
            "message_ms_total": 0.0,
        }

    def _deserialize(self, message) -> InferenceResult:
        """Deserialize one Kafka value into an InferenceResult."""
        payload = message.value()

        if not payload:
            raise ValueError("Empty inference result message")

        result = InferenceResult()

        try:
            result.ParseFromString(payload)
        except DecodeError as error:
            raise ValueError(
                "Invalid inference result Protobuf message"
            ) from error

        return result

    def _validate_result(
        self,
        topic: str,
        result: InferenceResult,
    ) -> None:
        """Validate result metadata and ensure it matches its Kafka topic."""
        expected_model = self._topic_model_map.get(topic)

        if expected_model is None:
            raise ValueError(f"Unexpected Kafka topic: {topic}")

        if result.model_id != expected_model:
            raise ValueError(
                f"Model mismatch: expected={expected_model}, "
                f"received={result.model_id}"
            )

        if not result.camera_id:
            raise ValueError("Missing camera_id")
        if not result.frame_id:
            raise ValueError("Missing frame_id")
        if not result.status:
            raise ValueError("Missing inference status")
        if result.processed_at_ms <= 0:
            raise ValueError("Invalid processing timestamp")

        if result.is_event:
            if not result.event_type:
                raise ValueError("Missing event_type")
            if not result.storage_key:
                raise ValueError("Missing image storage_key")
            if result.captured_at_ms <= 0:
                raise ValueError("Invalid captured_at_ms")

    def _persist_batch(
        self,
        validated_messages: list[tuple[object, InferenceResult]],
    ) -> list[tuple[int | None, int]]:
        """
        Persist all validated results in one PostgreSQL transaction.

        The existing repositories are intentionally retained so their
        validation, unique constraints, conflict handling, and duplicate
        metadata checks remain in effect.
        """
        persisted_ids: list[tuple[int | None, int]] = []
        started_at = time.perf_counter()

        try:
            with self._database.transaction() as connection:
                with connection.cursor() as cursor:
                    for _message, result in validated_messages:
                        event_image_id = None

                        if result.is_event:
                            event_image_id = self._image_repository.save(
                                cursor=cursor,
                                result=result,
                            )

                        result_id = self._results_repository.save(
                            cursor=cursor,
                            result=result,
                            event_image_id=event_image_id,
                        )

                        persisted_ids.append((event_image_id, result_id))

            return persisted_ids
        finally:
            self._metrics["db_transaction_ms_total"] += (
                time.perf_counter() - started_at
            ) * 1000.0
            self._metrics["db_transaction_count"] += 1

    def _process_batch(self, messages: list) -> None:
        """Validate, persist, and commit one complete batch."""
        if not messages:
            return

        batch_started_at = time.perf_counter()

        if self._first_message_at is None:
            self._first_message_at = batch_started_at

        batch_size = len(messages)
        self._metrics["messages"] += batch_size
        self._metrics["batches"] += 1
        self._metrics["batch_size_total"] += batch_size
        self._metrics["max_batch_size"] = max(
            self._metrics["max_batch_size"],
            batch_size,
        )

        try:
            validated_messages: list[tuple[object, InferenceResult]] = []

            # Validate the entire batch before opening the database transaction.
            for message in messages:
                result = self._deserialize(message)
                self._validate_result(
                    topic=message.topic(),
                    result=result,
                )
                validated_messages.append((message, result))

            # One transaction for all image and inference metadata in this batch.
            self._persist_batch(validated_messages)

            # Advance Kafka offsets only after the database transaction succeeds.
            commit_started_at = time.perf_counter()
            try:
                self._consumer.commit_batch(messages)
            finally:
                self._metrics["offset_commit_ms_total"] += (
                    time.perf_counter() - commit_started_at
                ) * 1000.0
                self._metrics["offset_commit_count"] += 1

            self._metrics["processed"] += batch_size

            event_count = sum(
                1 for _message, result in validated_messages
                if result.is_event
            )
            topic_counts: dict[str, int] = {}
            for message in messages:
                topic = message.topic()
                topic_counts[topic] = topic_counts.get(topic, 0) + 1

            print(
                "[RESULT BATCH SAVED] "
                f"messages={batch_size}, "
                f"events={event_count}, "
                f"topics={topic_counts}",
                flush=True,
            )

        except Exception:
            self._metrics["failed"] += batch_size
            raise

        finally:
            finished_at = time.perf_counter()
            self._last_message_at = finished_at
            self._metrics["message_ms_total"] += (
                finished_at - batch_started_at
            ) * 1000.0

    def _metrics_snapshot(self) -> dict:
        now = time.perf_counter()

        elapsed = max(
            now - self._run_started_at,
            1e-9,
        )

        active_elapsed = 0.0
        if self._first_message_at is not None:
            active_elapsed = max(
                (self._last_message_at or now) - self._first_message_at,
                self._metrics["message_ms_total"] / 1000.0,
                1e-9,
            )

        batch_count = self._metrics["batches"]
        message_count = self._metrics["messages"]
        transaction_count = self._metrics["db_transaction_count"]
        commit_count = self._metrics["offset_commit_count"]

        return {
            "pid": os.getpid(),
            "elapsed_seconds": round(elapsed, 3),
            "active_elapsed_seconds": round(active_elapsed, 3),
            "messages": message_count,
            "processed": self._metrics["processed"],
            "failed": self._metrics["failed"],
            "batches": batch_count,
            "batch_size_avg": round(
                self._metrics["batch_size_total"] / max(batch_count, 1),
                3,
            ),
            "batch_size_max": self._metrics["max_batch_size"],
            "results_fps": round(
                self._metrics["processed"] / active_elapsed,
                3,
            ) if active_elapsed else 0.0,
            "message_avg_ms": round(
                self._metrics["message_ms_total"] / max(message_count, 1),
                3,
            ),
            # This is now average transaction latency per batch.
            "db_transaction_avg_ms": round(
                self._metrics["db_transaction_ms_total"]
                / max(transaction_count, 1),
                3,
            ),
            "db_transaction_count": transaction_count,
            "db_transaction_avg_ms_per_message": round(
                self._metrics["db_transaction_ms_total"]
                / max(self._metrics["processed"], 1),
                3,
            ),
            # This is now average synchronous commit latency per batch.
            "offset_commit_avg_ms": round(
                self._metrics["offset_commit_ms_total"]
                / max(commit_count, 1),
                3,
            ),
            "offset_commit_count": commit_count,
        }

    def _log_metrics(self) -> None:
        print(
            "[LOAD_TEST_RESULTS_METRICS] "
            + json.dumps(self._metrics_snapshot(), sort_keys=True),
            flush=True,
        )

    def run(self, max_messages: int | None = None) -> None:
        """Continuously consume and persist result batches."""
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be positive or None")

        handled = 0
        next_metrics_log = 100
        self._run_started_at = time.perf_counter()

        print("Results Processing Service started.")
        print("Subscribed topics:", list(self._topic_model_map.keys()))
        print(
            "Batch configuration: "
            f"size={self._batch_size}, "
            f"window_ms={self._batch_timeout_ms}, "
            f"poll_timeout_seconds={self._poll_timeout_seconds}"
        )

        try:
            while max_messages is None or handled < max_messages:
                remaining = (
                    max_messages - handled
                    if max_messages is not None
                    else self._batch_size
                )
                current_batch_limit = min(self._batch_size, remaining)

                messages = self._consumer.poll_batch(
                    max_messages=current_batch_limit,
                    timeout=self._poll_timeout_seconds,
                    batch_window_ms=self._batch_timeout_ms,
                )

                if not messages:
                    continue

                try:
                    self._process_batch(messages)
                except Exception as error:
                    first = messages[0]
                    print(
                        "[RESULT BATCH ERROR] "
                        f"batch_size={len(messages)}, "
                        f"first_topic={first.topic()}, "
                        f"first_partition={first.partition()}, "
                        f"first_offset={first.offset()}, "
                        f"error={error}",
                        flush=True,
                    )
                    # Stop without intentionally committing a failed batch.
                    raise

                handled += len(messages)

                if handled >= next_metrics_log:
                    self._log_metrics()
                    next_metrics_log = ((handled // 100) + 1) * 100

        finally:
            try:
                self._consumer.close()
            finally:
                self._log_metrics()
                print("\nResults Processing Service stopped.")
                print(
                    f"Processed={self._metrics['processed']}, "
                    f"Failed={self._metrics['failed']}, "
                    f"Batches={self._metrics['batches']}"
                )
