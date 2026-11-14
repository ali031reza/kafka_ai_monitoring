"""Route DeepStream Protobuf results through the existing Kafka producer."""

from typing import TYPE_CHECKING

from src.proto.inference_result_pb2 import InferenceResult

if TYPE_CHECKING:
    from src.kafka.result_producer import KafkaResultProducer


class DeepStreamResultPublisher:
    """Publish model results without duplicating Kafka producer behavior.

    The producer is the existing KafkaResultProducer, which already uses
    acks=all, idempotence, and delivery confirmation.
    """

    def __init__(
        self,
        producer: "KafkaResultProducer",
        result_topics: dict[str, str],
    ) -> None:
        if not result_topics:
            raise ValueError("At least one model result topic is required")
        if any(not model_id or not topic for model_id, topic in result_topics.items()):
            raise ValueError("Model IDs and result topics must be non-empty")
        if len(set(result_topics.values())) != len(result_topics):
            raise ValueError("Each model must have a distinct result topic")

        self._producer = producer
        self._result_topics = dict(result_topics)

    def publish(self, result: InferenceResult) -> None:
        topic = self._result_topics.get(result.model_id)
        if topic is None:
            raise ValueError(f"No result topic configured for model {result.model_id!r}")
        self._producer.publish(topic, result)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.close()
