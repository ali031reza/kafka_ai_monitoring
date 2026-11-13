from confluent_kafka import Producer

from src.ingestion.frame_router import FrameRouter
from src.ingestion.frame_serializer import FrameSerializer


class KafkaFrameProducer:
    """Publishes frames to every model topic assigned to a camera."""

    def __init__(
        self,
        kafka_config: dict,
        router: FrameRouter,
        serializer: FrameSerializer,
    ):
        self._router = router
        self._serializer = serializer
        self._delivery_errors = []
        self._delivered_since_flush = 0

        settings = kafka_config["producer"]
        producer_config = {
            "bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
            "acks": settings["acks"],
            "enable.idempotence": settings["enable_idempotence"],
            "compression.type": settings["compression_type"],
            "batch.size": settings["batch_size"],
            "linger.ms": settings["linger_ms"],
        }
        if "delivery_timeout_ms" in settings:
            producer_config["delivery.timeout.ms"] = settings[
                "delivery_timeout_ms"
            ]

        self._producer = Producer(producer_config)

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
            while True:
                try:
                    self._producer.produce(
                        topic=topic,
                        key=camera_id.encode("utf-8"),
                        value=payload,
                        on_delivery=self._on_delivery,
                    )
                    published += 1
                    break
                except BufferError:
                    self._producer.poll(0.1)

        return published

    def _on_delivery(self, error, message):
        if error is not None:
            self._delivery_errors.append(str(error))
        else:
            self._delivered_since_flush += 1

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining:
            raise RuntimeError(
                f"{remaining} Kafka frame messages remain pending"
            )

        if self._delivery_errors:
            errors = self._delivery_errors[:]
            self._delivery_errors.clear()
            raise RuntimeError(
                f"Kafka frame delivery errors: {errors[:10]}"
            )

        if self._delivered_since_flush:
            print(
                f"[KAFKA FRAME FLUSH] "
                f"Delivered={self._delivered_since_flush}"
            )
            self._delivered_since_flush = 0
