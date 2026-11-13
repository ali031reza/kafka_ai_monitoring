from confluent_kafka import Consumer, KafkaException


class KafkaFrameConsumer:
    """Reads frames from Kafka and manages manual offset commits."""

    def __init__(
        self,
        bootstrap_servers: list[str],
        topic: str,
        group_id: str,
        offset_reset: str = "earliest",
    ):
        self._consumer = Consumer({
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })

        self._consumer.subscribe([topic])

    def poll(self, timeout: float = 1.0):
        message = self._consumer.poll(timeout)

        if message is not None and message.error():
            raise KafkaException(message.error())

        return message

    def commit(self, message) -> None:
        """Commit only after successful downstream processing."""
        self._consumer.commit(
            message=message,
            asynchronous=False,
        )

    def close(self) -> None:
        self._consumer.close()