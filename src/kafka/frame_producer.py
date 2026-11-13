from confluent_kafka import Producer

from src.ingestion.frame_router import FrameRouter
from src.ingestion.frame_serializer import FrameSerializer


class KafkaFrameProducer:
    """Publishes camera frames to their assigned Kafka topics."""

    def __init__(
        self,
        kafka_config: dict,
        router: FrameRouter,
        serializer: FrameSerializer,
    ):
        self._router = router
        self._serializer = serializer
        self._delivery_errors = []

        settings = kafka_config["producer"]

        self._producer = Producer({
            "bootstrap.servers": ",".join(
                kafka_config["bootstrap_servers"]
            ),
            "acks": settings["acks"],
            "enable.idempotence": settings["enable_idempotence"],
            "compression.type": settings["compression_type"],
            "batch.size": settings["batch_size"],
            "linger.ms": settings["linger_ms"],
        })

    def publish_frame(
        self,
        camera_id: str,
        frame_id: str,
        timestamp_ms: int,
        sequence_number: int,
        image_data: bytes,
    ) -> int:

        topics = self._router.get_topics(camera_id)

        if not topics:
            raise ValueError(
                f"No topics configured for camera {camera_id}"
            )

        payload = self._serializer.serialize(
            camera_id=camera_id,
            frame_id=frame_id,
            timestamp_ms=timestamp_ms,
            sequence_number=sequence_number,
            image_data=image_data,
        )

        self._producer.poll(0)

        published = 0

        for topic in topics:
            self._producer.produce(
                topic=topic,
                key=camera_id.encode("utf-8"),
                value=payload,
                on_delivery=self._on_delivery,
            )

            published += 1

        return published

    def _on_delivery(self, error, message) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))
            print(f"Delivery failed: {error}")
            return

        print(
            f"Delivered to {message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)

        if remaining:
            raise RuntimeError(
                f"{remaining} messages still pending"
            )

        if self._delivery_errors:
            raise RuntimeError(
                f"Kafka delivery errors: {self._delivery_errors}"
            )