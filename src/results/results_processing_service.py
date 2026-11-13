
from google.protobuf.message import DecodeError

from src.proto.inference_result_pb2 import InferenceResult

from src.kafka.results_consumer import KafkaResultsConsumer

from src.database.client import DatabaseClient
from src.database.image_repository import ImageRepository
from src.database.results_repository import ResultsRepository


class ResultsProcessingService:
    """
    Processes inference results received from Kafka.

    Responsibilities:
        - Consume inference results
        - Deserialize Protobuf messages
        - Validate model-topic relationships
        - Persist event image metadata
        - Persist inference metadata
        - Manage PostgreSQL transactions
        - Commit Kafka offsets after database commits
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
        """
        Save image and inference metadata atomically.
        """

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

    def _process_message(self, message) -> None:
        """Process one inference result."""

        result = self._deserialize(message)

        self._validate_result(
            topic=message.topic(),
            result=result,
        )

        event_image_id, result_id = self._persist_result(
            result=result
        )

        # Commit only after successful PostgreSQL transaction.
        self._consumer.commit(message)

        print(
            f"[RESULT SAVED] "
            f"Model={result.model_id}, "
            f"Camera={result.camera_id}, "
            f"Frame={result.frame_id}, "
            f"Result ID={result_id}, "
            f"Image ID={event_image_id}"
        )

    def run(
        self,
        max_messages: int | None = None,
    ) -> None:
        """Continuously process Kafka inference results."""

        handled = 0
        processed = 0
        failed = 0

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

                message = self._consumer.poll(
                    timeout=1.0
                )

                if message is None:
                    continue

                try:

                    self._process_message(message)

                    processed += 1
                    handled += 1

                except Exception as error:

                    failed += 1

                    print(
                        f"[RESULT PROCESSING ERROR] "
                        f"Topic={message.topic()}, "
                        f"Partition={message.partition()}, "
                        f"Offset={message.offset()}, "
                        f"Error={error}"
                    )

                    # Do not commit failed messages.
                    # Stop to avoid skipping an unprocessed offset.
                    raise

        finally:

            self._consumer.close()

            print("\nResults Processing Service stopped.")

            print(
                f"Processed={processed}, Failed={failed}"
            )

