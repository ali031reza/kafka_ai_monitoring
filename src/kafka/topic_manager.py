from confluent_kafka.admin import AdminClient, NewTopic


class KafkaTopicManager:
    """
    Manages Kafka topics using application configuration.

    Responsibilities:
        - Connect to Kafka
        - Check existing topics
        - Create missing frame topics
        - Create missing result topics
        - Create the Dead Letter Topic
        - Validate partition counts
    """

    def __init__(self, config: dict):
        self._config = config

        self._admin = AdminClient({
            "bootstrap.servers": ",".join(
                config["bootstrap_servers"]
            )
        })

    def _build_topic(
        self,
        name: str,
        partitions: int,
        retention_ms: int,
    ) -> NewTopic:
        """Create a Kafka topic definition."""

        return NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=int(
                self._config["replication_factor"]
            ),
            config={
                "retention.ms": str(retention_ms)
            },
        )

    def _check_or_prepare_topic(
        self,
        name: str,
        partitions: int,
        retention_ms: int,
        existing_topics: dict,
        new_topics: list,
    ) -> None:
        """
        Check whether a topic exists.

        If it exists, validate its partition count.
        Otherwise, prepare it for creation.
        """

        if name in existing_topics:
            existing_topic = existing_topics[name]

            if existing_topic.error is not None:
                raise RuntimeError(
                    f"Cannot inspect topic {name}: "
                    f"{existing_topic.error}"
                )

            existing_partitions = len(
                existing_topic.partitions
            )

            if existing_partitions != partitions:
                raise ValueError(
                    f"Partition mismatch for {name}: "
                    f"configured={partitions}, "
                    f"actual={existing_partitions}"
                )

            print(f"Already exists: {name}")
            return

        new_topics.append(
            self._build_topic(
                name=name,
                partitions=partitions,
                retention_ms=retention_ms,
            )
        )

    def ensure_topics(self) -> None:
        """Ensure all configured Kafka topics exist."""

        # 1. Retrieve current Kafka topics
        metadata = self._admin.list_topics(timeout=10)
        existing_topics = metadata.topics

        new_topics = []

        # 2. Check frame and result topics
        for model_name, settings in self._config["topics"].items():

            partitions = int(settings["partitions"])

            for kind in ("frames", "results"):
                topic_name = settings[kind]

                retention_ms = int(
                    self._config["retention"][f"{kind}_ms"]
                )

                self._check_or_prepare_topic(
                    name=topic_name,
                    partitions=partitions,
                    retention_ms=retention_ms,
                    existing_topics=existing_topics,
                    new_topics=new_topics,
                )

        # 3. Check Dead Letter Topic
        dlt_config = self._config["dead_letter"]

        self._check_or_prepare_topic(
            name=dlt_config["topic"],
            partitions=int(dlt_config["partitions"]),
            retention_ms=int(dlt_config["retention_ms"]),
            existing_topics=existing_topics,
            new_topics=new_topics,
        )

        # 4. No missing topics
        if not new_topics:
            print("All Kafka topics already exist.")
            return

        # 5. Create missing topics
        futures = self._admin.create_topics(
            new_topics,
            request_timeout=30,
        )

        for topic_name, future in futures.items():
            try:
                future.result(timeout=30)
                print(f"Created: {topic_name}")

            except Exception as error:
                raise RuntimeError(
                    f"Failed to create topic {topic_name}: "
                    f"{error}"
                ) from error

        print("Kafka topic setup completed.")