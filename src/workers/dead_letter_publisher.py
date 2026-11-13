import time

from src.kafka.result_producer import KafkaResultProducer
from src.proto.dead_letter_pb2 import DeadLetterEvent


class DeadLetterPublisher:
    """
    Publishes failed message metadata to Kafka.

    Responsibilities:
        - Build DeadLetterEvent messages
        - Capture failure information
        - Publish events to the DLT
        - Wait for Kafka delivery confirmation
    """

    def __init__(
        self,
        producer: KafkaResultProducer,
        topic: str,
        model_id: str,
    ):
        self._producer = producer
        self._topic = topic
        self._model_id = model_id

    def publish_failure(
        self,
        message,
        error: Exception,
        frame=None,
        attempts: int = 0,
    ) -> None:

        event = DeadLetterEvent(
            camera_id=frame.camera_id if frame else "",
            frame_id=frame.frame_id if frame else "",
            model_id=self._model_id,

            source_topic=message.topic(),
            source_partition=message.partition(),
            source_offset=message.offset(),

            error_type=type(error).__name__,
            error_message=str(error)[:500],

            attempts=attempts,
            failed_at_ms=time.time_ns() // 1_000_000,
        )

        self._producer.publish(
            self._topic,
            event,
        )

        print(
            f"[DLT] {self._model_id}: "
            f"{type(error).__name__}: {error}"
        )