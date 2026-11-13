
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
    """
    Initializes the Kafka AI Monitoring Results Application.

    Responsibilities:
        - Load application configuration
        - Initialize PostgreSQL database client
        - Discover configured inference result topics
        - Initialize Kafka results consumer
        - Initialize database repositories
        - Inject dependencies into processing service
        - Start the results processing application
    """

    def __init__(self):

        # ==========================================
        # 1. LOAD APPLICATION CONFIGURATION
        # ==========================================

        project_root = Path(__file__).resolve().parents[2]

        self._config_manager = ConfigManager(project_root)

        self._kafka_config = (
            self._config_manager.get_section("kafka")
        )

        print("Loading Results Processing Application...")

        # ==========================================
        # 2. INITIALIZE DATABASE
        # ==========================================

        self._database = DatabaseClient(
            **self._config_manager.get_database_config()
        )

        print("PostgreSQL database client initialized.")

        # ==========================================
        # 3. BUILD RESULT TOPIC MAPPING
        # ==========================================

        model_configs = self._kafka_config["topics"]

        self._topic_model_map = {}

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
            raise ValueError(
                "No inference result topics configured"
            )

        print(
            f"Configured result topics: "
            f"{list(self._topic_model_map.keys())}"
        )

        # ==========================================
        # 4. INITIALIZE KAFKA RESULTS CONSUMER
        # ==========================================

        results_config = self._kafka_config.get(
            "results_consumer",
            {},
        )

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
            bootstrap_servers=self._kafka_config[
                "bootstrap_servers"
            ],
            topics=list(self._topic_model_map.keys()),
            group_id=group_id,
            offset_reset=offset_reset,
        )

        # ==========================================
        # 5. INITIALIZE DATABASE REPOSITORIES
        # ==========================================

        self._image_repository = ImageRepository()

        self._results_repository = ResultsRepository()

        print("Database repositories initialized.")

        # ==========================================
        # 6. INITIALIZE RESULTS PROCESSING SERVICE
        # ==========================================

        self._service = ResultsProcessingService(
            consumer=self._consumer,
            database=self._database,
            image_repository=self._image_repository,
            results_repository=self._results_repository,
            topic_model_map=self._topic_model_map,
        )

        print("Results Processing Application initialized.")

        print(f"Kafka consumer group: {group_id}")

    # ==========================================
    # 7. RUN APPLICATION
    # ==========================================

    def run(
        self,
        max_messages: int | None = None,
    ) -> None:
        """
        Start the Results Processing Service.

        Args:
            max_messages:
                None -> Run continuously.
                N    -> Process N messages and exit.
        """

        print("\nStarting Results Processing Service...")

        self._service.run(
            max_messages=max_messages
        )
