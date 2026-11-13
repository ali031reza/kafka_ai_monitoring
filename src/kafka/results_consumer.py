
from confluent_kafka import Consumer, KafkaException


class KafkaResultsConsumer:
    """
    Consumes inference results from multiple Kafka topics.

    Responsibilities:
        - Subscribe to all AI model result topics
        - Poll Kafka for inference results
        - Handle Kafka consumer errors
        - Manage manual offset commits
        - Close consumer resources safely

    Offset commits are performed manually by the
    ResultsProcessingService after successful
    PostgreSQL persistence.
    """

    def __init__(
        self,
        bootstrap_servers: list[str],
        topics: list[str],
        group_id: str,
        offset_reset: str = "earliest",
    ):
        """
        Initialize the Kafka results consumer.

        Args:
            bootstrap_servers:
                Kafka broker addresses.

            topics:
                List of inference result topics.

            group_id:
                Kafka consumer group ID.

            offset_reset:
                Offset reset policy: earliest or latest.
        """

        # ==========================================
        # 1. VALIDATE CONFIGURATION
        # ==========================================

        if not bootstrap_servers:
            raise ValueError(
                "Kafka bootstrap servers cannot be empty"
            )

        if not topics:
            raise ValueError(
                "At least one result topic is required"
            )

        if not group_id:
            raise ValueError(
                "Kafka consumer group ID cannot be empty"
            )

        if offset_reset not in ("earliest", "latest", "error"):
            raise ValueError(
                f"Invalid offset reset policy: {offset_reset}"
            )

        self._topics = list(dict.fromkeys(topics))
        self._group_id = group_id
        self._closed = False

        # ==========================================
        # 2. INITIALIZE KAFKA CONSUMER
        # ==========================================

        consumer_config = {
            "bootstrap.servers": ",".join(bootstrap_servers),
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }

        self._consumer = Consumer(consumer_config)

        # ==========================================
        # 3. SUBSCRIBE TO RESULT TOPICS
        # ==========================================

        self._consumer.subscribe(self._topics)

        print(
            "[KAFKA RESULTS CONSUMER INITIALIZED]"
        )

        print(f"Consumer group: {self._group_id}")

        print(f"Subscribed topics: {self._topics}")

    # ==========================================
    # 4. POLL KAFKA
    # ==========================================

    def poll(self, timeout: float = 1.0):
        """
        Retrieve one inference result from Kafka.

        Args:
            timeout:
                Maximum time to wait in seconds.

        Returns:
            Kafka message if available.
            None if no message is available.

        Raises:
            KafkaException:
                If Kafka reports a consumer error.
        """

        if self._closed:
            raise RuntimeError(
                "Cannot poll a closed Kafka consumer"
            )

        message = self._consumer.poll(timeout)

        if message is None:
            return None

        if message.error():
            raise KafkaException(message.error())

        return message

    # ==========================================
    # 5. COMMIT KAFKA OFFSET
    # ==========================================

    def commit(self, message) -> None:
        """
        Commit a Kafka message offset synchronously.

        This method must only be called after
        successful PostgreSQL persistence.

        Args:
            message:
                Successfully processed Kafka message.
        """

        if self._closed:
            raise RuntimeError(
                "Cannot commit using a closed Kafka consumer"
            )

        self._consumer.commit(
            message=message,
            asynchronous=False,
        )

    # ==========================================
    # 6. CLOSE CONSUMER
    # ==========================================

    def close(self) -> None:
        """
        Close Kafka consumer and release resources.

        Safe to call multiple times.
        """

        if self._closed:
            return

        self._consumer.close()

        self._closed = True

        print(
            "[KAFKA RESULTS CONSUMER CLOSED]"
        )

    # ==========================================
    # 7. CONTEXT MANAGER SUPPORT
    # ==========================================

    def __enter__(self):
        """Allow usage with Python's with statement."""

        return self

    def __exit__(
        self,
        exception_type,
        exception_value,
        traceback,
    ):
        """Automatically close the Kafka consumer."""

        self.close()

        return False
