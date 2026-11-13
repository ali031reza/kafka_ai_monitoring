from confluent_kafka import Consumer, KafkaException


class KafkaFrameConsumer:
    """Consumes frames with manual offset commits."""

    def __init__(
        self,
        bootstrap_servers: list[str],
        topic: str,
        group_id: str,
        offset_reset: str = "earliest",
        client_id: str | None = None,
    ):
        if not bootstrap_servers:
            raise ValueError("Kafka bootstrap servers cannot be empty")
        if not topic or not group_id:
            raise ValueError("Kafka topic and group_id are required")

        config = {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
        if client_id:
            config["client.id"] = client_id

        self._consumer = Consumer(config)
        self._closed = False
        self._consumer.subscribe([topic])

    def poll(self, timeout: float = 1.0):
        if self._closed:
            raise RuntimeError("Cannot poll a closed consumer")
        message = self._consumer.poll(timeout)
        if message is not None and message.error():
            raise KafkaException(message.error())
        return message

    def commit(self, message) -> None:
        if self._closed:
            raise RuntimeError("Cannot commit with a closed consumer")
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        if not self._closed:
            self._consumer.close()
            self._closed = True
