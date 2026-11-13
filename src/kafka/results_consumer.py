import time

from confluent_kafka import Consumer, KafkaException, TopicPartition


class KafkaResultsConsumer:
    """
    Consumes inference results from multiple Kafka topics.

    PostgreSQL persistence is performed by ResultsProcessingService.
    Offsets are committed manually only after the corresponding database
    transaction has completed successfully.
    """

    def __init__(
        self,
        bootstrap_servers: list[str],
        topics: list[str],
        group_id: str,
        offset_reset: str = "earliest",
    ):
        if not bootstrap_servers:
            raise ValueError("Kafka bootstrap servers cannot be empty")
        if not topics:
            raise ValueError("At least one result topic is required")
        if not group_id:
            raise ValueError("Kafka consumer group ID cannot be empty")
        if offset_reset not in ("earliest", "latest", "error"):
            raise ValueError(f"Invalid offset reset policy: {offset_reset}")

        self._topics = list(dict.fromkeys(topics))
        self._group_id = group_id
        self._closed = False

        self._consumer = Consumer({
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })

        self._consumer.subscribe(self._topics)

        print("[KAFKA RESULTS CONSUMER INITIALIZED]")
        print(f"Consumer group: {self._group_id}")
        print(f"Subscribed topics: {self._topics}")

    def poll(self, timeout: float = 1.0):
        """Retrieve one result message, or return None on timeout."""
        if self._closed:
            raise RuntimeError("Cannot poll a closed Kafka consumer")

        message = self._consumer.poll(timeout)

        if message is None:
            return None

        if message.error():
            raise KafkaException(message.error())

        return message

    def poll_batch(
        self,
        max_messages: int = 100,
        timeout: float = 1.0,
        batch_window_ms: int = 20,
    ) -> list:
        """
        Collect up to max_messages.

        timeout is the maximum wait for the first message.
        batch_window_ms is the maximum additional accumulation window after
        the first message arrives.
        """
        if max_messages <= 0:
            raise ValueError("max_messages must be positive")
        if timeout < 0:
            raise ValueError("timeout cannot be negative")
        if batch_window_ms < 0:
            raise ValueError("batch_window_ms cannot be negative")

        first_message = self.poll(timeout=timeout)
        if first_message is None:
            return []

        messages = [first_message]
        deadline = time.monotonic() + (batch_window_ms / 1000.0)

        while len(messages) < max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            message = self.poll(timeout=remaining)
            if message is None:
                break

            messages.append(message)

        return messages

    def commit(self, message) -> None:
        """Commit one message offset synchronously."""
        if self._closed:
            raise RuntimeError("Cannot commit using a closed Kafka consumer")

        self._consumer.commit(
            message=message,
            asynchronous=False,
        )

    def commit_batch(self, messages: list) -> None:
        """
        Commit the next offset for each partition represented in messages.

        The caller must invoke this only after every message in the batch has
        been persisted successfully. For each topic/partition, the highest
        message offset in the batch is committed as offset + 1.
        """
        if self._closed:
            raise RuntimeError("Cannot commit using a closed Kafka consumer")
        if not messages:
            return

        next_offsets: dict[tuple[str, int], int] = {}

        for message in messages:
            key = (message.topic(), message.partition())
            next_offset = message.offset() + 1
            current = next_offsets.get(key)

            if current is None or next_offset > current:
                next_offsets[key] = next_offset

        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in next_offsets.items()
        ]

        self._consumer.commit(
            offsets=offsets,
            asynchronous=False,
        )

    def close(self) -> None:
        """Close the Kafka consumer; safe to call more than once."""
        if self._closed:
            return

        self._consumer.close()
        self._closed = True
        print("[KAFKA RESULTS CONSUMER CLOSED]")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
        return False
