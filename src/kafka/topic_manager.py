from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic


class KafkaTopicManager:
    """Creates missing topics and increases existing partition counts."""

    def __init__(self, config: dict):
        self._config = config
        self._admin = AdminClient({
            "bootstrap.servers": ",".join(config["bootstrap_servers"])
        })

    def _build_topic(self, name, partitions, retention_ms):
        return NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=int(self._config["replication_factor"]),
            config={"retention.ms": str(retention_ms)},
        )

    def _check_topic(
        self,
        name,
        desired_partitions,
        retention_ms,
        existing_topics,
        new_topics,
        partition_increases,
    ):
        if name not in existing_topics:
            new_topics.append(
                self._build_topic(name, desired_partitions, retention_ms)
            )
            return

        metadata = existing_topics[name]
        if metadata.error is not None:
            raise RuntimeError(
                f"Cannot inspect topic {name}: {metadata.error}"
            )

        current = len(metadata.partitions)
        if current < desired_partitions:
            print(
                f"Partition increase planned: {name} {current} -> "
                f"{desired_partitions}"
            )
            partition_increases.append(
                NewPartitions(name, desired_partitions)
            )
        elif current > desired_partitions:
            print(
                f"Keeping {name} at {current} partitions; Kafka cannot "
                f"decrease it to {desired_partitions}."
            )
        else:
            print(f"Already configured: {name} ({current} partitions)")

    def ensure_topics(self):
        metadata = self._admin.list_topics(timeout=10)
        existing = metadata.topics
        new_topics = []
        increases = []

        for settings in self._config["topics"].values():
            frame_partitions = int(settings.get("partitions", 4))
            result_partitions = int(
                settings.get("results_partitions", settings.get("partitions", 4))
            )

            for kind, partitions in (
                ("frames", frame_partitions),
                ("results", result_partitions),
            ):
                self._check_topic(
                    name=settings[kind],
                    desired_partitions=partitions,
                    retention_ms=int(
                        self._config["retention"][f"{kind}_ms"]
                    ),
                    existing_topics=existing,
                    new_topics=new_topics,
                    partition_increases=increases,
                )

        dlt = self._config["dead_letter"]
        self._check_topic(
            name=dlt["topic"],
            desired_partitions=int(dlt["partitions"]),
            retention_ms=int(dlt["retention_ms"]),
            existing_topics=existing,
            new_topics=new_topics,
            partition_increases=increases,
        )

        if new_topics:
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
                        f"Failed to create topic {topic_name}: {error}"
                    ) from error

        if increases:
            print(
                "WARNING: Increasing partitions can remap keyed records. "
                "Review per-camera ordering assumptions."
            )
            futures = self._admin.create_partitions(
                increases,
                request_timeout=30,
            )
            for topic_name, future in futures.items():
                try:
                    future.result(timeout=30)
                    print(f"Partitions increased: {topic_name}")
                except Exception as error:
                    raise RuntimeError(
                        f"Failed to increase partitions for "
                        f"{topic_name}: {error}"
                    ) from error

        if not new_topics and not increases:
            print("All Kafka topics already match the configuration.")

        print("Kafka topic setup completed.")
