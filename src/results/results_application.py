from pathlib import Path

from config.config_manager import ConfigManager

from src.database.client import DatabaseClient
from src.database.image_repository import ImageRepository
from src.database.results_repository import ResultsRepository

from src.kafka.results_consumer import KafkaResultsConsumer

from src.results.results_processing_service import (
    ResultsProcessingService,
)


class ResultsApplication:
    """Initializes and runs the Kafka AI Monitoring Results Application."""

    def __init__(self):
        project_root = Path(__file__).resolve().parents[2]
        self._config_manager = ConfigManager(project_root)

        self._kafka_config = self._config_manager.get_section("kafka")
        print("Loading Results Processing Application...")

        self._database = DatabaseClient(
            **self._config_manager.get_database_config()
        )
        print("PostgreSQL database client initialized.")

        model_configs = self._kafka_config["topics"]
        self._topic_model_map: dict[str, str] = {}

        for model_id, model_config in model_configs.items():
            result_topic = model_config.get("results")

            if not result_topic:
                raise ValueError(
                    f"Missing results topic for model: {model_id}"
                )

            if result_topic in self._topic_model_map:
                raise ValueError(
                    f"Duplicate results topic: {result_topic}"
                )

            self._topic_model_map[result_topic] = model_id

        if not self._topic_model_map:
            raise ValueError("No inference result topics configured")

        print(
            f"Configured result topics: "
            f"{list(self._topic_model_map.keys())}"
        )

        results_config = self._kafka_config.get("results_consumer", {})
        consumer_config = self._kafka_config["consumer"]

        group_id = results_config.get(
            "group_id",
            "results_processor_group",
        )
        offset_reset = results_config.get(
            "offset_reset",
            consumer_config["offset_reset"],
        )

        self._consumer = KafkaResultsConsumer(
            bootstrap_servers=self._kafka_config["bootstrap_servers"],
            topics=list(self._topic_model_map.keys()),
            group_id=group_id,
            offset_reset=offset_reset,
        )

        self._image_repository = ImageRepository()
        self._results_repository = ResultsRepository()

        batch_size = int(results_config.get("batch_size", 100))
        batch_timeout_ms = int(
            results_config.get("batch_timeout_ms", 20)
        )
        poll_timeout_seconds = float(
            results_config.get("poll_timeout_seconds", 1.0)
        )

        self._service = ResultsProcessingService(
            consumer=self._consumer,
            database=self._database,
            image_repository=self._image_repository,
            results_repository=self._results_repository,
            topic_model_map=self._topic_model_map,
            batch_size=batch_size,
            batch_timeout_ms=batch_timeout_ms,
            poll_timeout_seconds=poll_timeout_seconds,
        )

        print("Database repositories initialized.")
        print("Results Processing Application initialized.")
        print(f"Kafka consumer group: {group_id}")
        print(
            "Results batching configured: "
            f"batch_size={batch_size}, "
            f"batch_timeout_ms={batch_timeout_ms}"
        )

    def run(self, max_messages: int | None = None) -> None:
        """Start the Results Processing Service."""
        print("\nStarting Results Processing Service...")
        self._service.run(max_messages=max_messages)
