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
    Processes inference results received from Kafka.

    Load-test instrumentation reports total result throughput, database
    transaction latency, and offset-commit latency without changing the
    persistence or offset-commit ordering.
    """

    def __init__(
        self,
        consumer: KafkaResultsConsumer,
        database: DatabaseClient,
        image_repository: ImageRepository,
        results_repository: ResultsRepository,
        topic_model_map: dict[str, str],
    ):
        if not topic_model_map:
            raise ValueError("No result topics configured")

        self._consumer = consumer
        self._database = database
        self._image_repository = image_repository
        self._results_repository = results_repository
        self._topic_model_map = topic_model_map
        self._run_started_at = None
        self._metrics = {
            "messages": 0,
            "processed": 0,
            "failed": 0,
            "db_transaction_ms_total": 0.0,
            "db_transaction_count": 0,
            "offset_commit_ms_total": 0.0,
            "offset_commit_count": 0,
            "message_ms_total": 0.0,
        }

    def _deserialize(self, message) -> InferenceResult:
        """Deserialize Kafka message into Protobuf."""
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
        """Validate inference result metadata."""
        expected_model = self._topic_model_map.get(topic)

        if expected_model is None:
            raise ValueError(
                f"Unexpected Kafka topic: {topic}"
            )

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
            if not result.captured_at_ms:
                raise ValueError("Missing captured_at_ms")

    def _persist_result(
        self,
        result: InferenceResult,
    ) -> tuple[int | None, int]:
        """Save image and inference metadata atomically."""
        started_at = time.perf_counter()
        try:
            with self._database.transaction() as connection:
                with connection.cursor() as cursor:
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

            return event_image_id, result_id
        finally:
            self._metrics["db_transaction_ms_total"] += (
                time.perf_counter() - started_at
            ) * 1000.0
            self._metrics["db_transaction_count"] += 1

    def _process_message(self, message) -> None:
        """Process one inference result and commit only after DB success."""
        started_at = time.perf_counter()
        try:
            result = self._deserialize(message)
            self._validate_result(
                topic=message.topic(),
                result=result,
            )

            event_image_id, result_id = self._persist_result(
                result=result
            )

            commit_started = time.perf_counter()
            try:
                self._consumer.commit(message)
            finally:
                self._metrics["offset_commit_ms_total"] += (
                    time.perf_counter() - commit_started
                ) * 1000.0
                self._metrics["offset_commit_count"] += 1

            self._metrics["processed"] += 1

            print(
                f"[RESULT SAVED] "
                f"Model={result.model_id}, "
                f"Camera={result.camera_id}, "
                f"Frame={result.frame_id}, "
                f"Result ID={result_id}, "
                f"Image ID={event_image_id}"
            )
        finally:
            self._metrics["messages"] += 1
            self._metrics["message_ms_total"] += (
                time.perf_counter() - started_at
            ) * 1000.0

    def _metrics_snapshot(self) -> dict:
        elapsed = max(
            time.perf_counter() - self._run_started_at,
            1e-9,
        )
        return {
            "pid": os.getpid(),
            "elapsed_seconds": round(elapsed, 3),
            "messages": self._metrics["messages"],
            "processed": self._metrics["processed"],
            "failed": self._metrics["failed"],
            "results_fps": round(
                self._metrics["processed"] / elapsed,
                3,
            ),
            "message_avg_ms": round(
                self._metrics["message_ms_total"]
                / max(self._metrics["messages"], 1),
                3,
            ),
            "db_transaction_avg_ms": round(
                self._metrics["db_transaction_ms_total"]
                / max(self._metrics["db_transaction_count"], 1),
                3,
            ),
            "db_transaction_count": self._metrics["db_transaction_count"],
            "offset_commit_avg_ms": round(
                self._metrics["offset_commit_ms_total"]
                / max(self._metrics["offset_commit_count"], 1),
                3,
            ),
            "offset_commit_count": self._metrics["offset_commit_count"],
        }

    def _log_metrics(self) -> None:
        print(
            "[LOAD_TEST_RESULTS_METRICS] "
            + json.dumps(self._metrics_snapshot(), sort_keys=True),
            flush=True,
        )

    def run(
        self,
        max_messages: int | None = None,
    ) -> None:
        """Continuously process Kafka inference results."""
        handled = 0
        self._run_started_at = time.perf_counter()

        print("Results Processing Service started.")
        print(
            "Subscribed topics:",
            list(self._topic_model_map.keys()),
        )

        try:
            while (
                max_messages is None
                or handled < max_messages
            ):
                message = self._consumer.poll(timeout=1.0)

                if message is None:
                    continue

                try:
                    self._process_message(message)
                    handled += 1
                except Exception as error:
                    self._metrics["failed"] += 1
                    print(
                        f"[RESULT PROCESSING ERROR] "
                        f"Topic={message.topic()}, "
                        f"Partition={message.partition()}, "
                        f"Offset={message.offset()}, "
                        f"Error={error}"
                    )
                    # Do not commit failed messages. Stop to avoid skipping
                    # an unprocessed offset.
                    raise

                if handled and handled % 100 == 0:
                    self._log_metrics()
        finally:
            try:
                self._consumer.close()
            finally:
                self._log_metrics()
                print("\nResults Processing Service stopped.")
                print(
                    f"Processed={self._metrics['processed']}, "
                    f"Failed={self._metrics['failed']}"
                )
