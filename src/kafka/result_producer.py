from confluent_kafka import Producer


class KafkaResultProducer:
    """Publishes inference results to Kafka."""

    def __init__(self, bootstrap_servers: list[str]):
        self._producer = Producer({
            "bootstrap.servers": ",".join(bootstrap_servers),
            "acks": "all",
            "enable.idempotence": True,
        })

    def publish(self, topic: str, result) -> None:
        delivery = {"confirmed": False, "error": None}

        def on_delivery(error, message):
            delivery["confirmed"] = error is None
            delivery["error"] = error

        self._producer.produce(
            topic=topic,
            key=result.camera_id.encode("utf-8"),
            value=result.SerializeToString(),
            on_delivery=on_delivery,
        )

        remaining = self._producer.flush(10)

        if remaining or not delivery["confirmed"]:
            raise RuntimeError(
                f"Result delivery failed: {delivery['error']}"
            )
            