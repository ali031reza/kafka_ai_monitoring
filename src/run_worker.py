import argparse
from pathlib import Path

from config.config_manager import ConfigManager

# Kafka components
from src.kafka.frame_consumer import KafkaFrameConsumer
from src.kafka.result_producer import KafkaResultProducer

# Serialization
from src.ingestion.frame_serializer import FrameSerializer

# AI worker components
from src.workers.ai_worker import AIWorker
from src.workers.dummy_model import DummyAIModel
from src.workers.freshness_policy import FrameFreshnessPolicy
from src.workers.dead_letter_publisher import DeadLetterPublisher

# Image storage
from src.storage.event_image_storage import EventImageStorage


class WorkerApplication:
    """
    Initializes and runs an AI model worker.

    Responsibilities:
        - Load application configuration
        - Initialize Kafka consumer and producer
        - Initialize Dead Letter Publisher
        - Initialize image storage
        - Initialize the selected AI model
        - Configure frame freshness policy
        - Configure retry policy
        - Start the AIWorker
    """

    def __init__(self, model_name: str):

        self._model_name = model_name

        # ==========================================
        # 1. LOAD CONFIGURATION
        # ==========================================

        project_root = Path(__file__).resolve().parents[1]

        config_manager = ConfigManager(project_root)

        kafka_config = config_manager.get_section("kafka")

        storage_config = config_manager.get_section("storage")[
            "event_images"
        ]

        # Retrieve selected model configuration
        model_config = kafka_config["topics"][model_name]

        bootstrap_servers = kafka_config["bootstrap_servers"]

        # ==========================================
        # 2. INITIALIZE KAFKA CONSUMER
        # ==========================================

        consumer = KafkaFrameConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=model_config["frames"],
            group_id=model_config["group_id"],
            offset_reset=kafka_config["consumer"]["offset_reset"],
        )

        # ==========================================
        # 3. INITIALIZE KAFKA PRODUCER
        # ==========================================

        producer = KafkaResultProducer(
            bootstrap_servers=bootstrap_servers
        )

        # ==========================================
        # 4. INITIALIZE DEAD LETTER PUBLISHER
        # ==========================================

        dead_letter_publisher = DeadLetterPublisher(
            producer=producer,
            topic=kafka_config["dead_letter"]["topic"],
            model_id=model_name,
        )

        # ==========================================
        # 5. INITIALIZE FRAME SERIALIZER
        # ==========================================

        serializer = FrameSerializer()

        # ==========================================
        # 6. INITIALIZE AI MODEL
        # ==========================================

        model = DummyAIModel(
            model_id=model_name
        )

        # ==========================================
        # 7. CONFIGURE FRAME FRESHNESS POLICY
        # ==========================================

        freshness_policy = FrameFreshnessPolicy(
            max_age_ms=model_config["max_frame_age_ms"]
        )

        # ==========================================
        # 8. INITIALIZE EVENT IMAGE STORAGE
        # ==========================================

        if not storage_config["enabled"]:
            raise RuntimeError(
                "Event image storage is disabled in configuration"
            )

        image_root = (
            project_root / storage_config["root_dir"]
        )

        image_storage = EventImageStorage(
            root_dir=image_root
        )

        print(
            f"Event image storage initialized: {image_root}"
        )

        # ==========================================
        # 9. INITIALIZE AI WORKER
        # ==========================================

        retry_config = kafka_config["retry"]

        self._worker = AIWorker(
            consumer=consumer,
            producer=producer,
            serializer=serializer,
            model=model,
            model_id=model_name,
            result_topic=model_config["results"],
            freshness_policy=freshness_policy,
            dead_letter_publisher=dead_letter_publisher,
            image_storage=image_storage,
            max_retries=retry_config["max_retries"],
            retry_delay_ms=retry_config["delay_ms"],
        )

    # ==========================================
    # 10. RUN WORKER
    # ==========================================

    def run(self, once: bool = False) -> None:
        """
        Start the AI worker.

        Args:
            once:
                True  -> Process one frame and exit.
                False -> Continue processing indefinitely.
        """

        print(f"Starting AI worker: {self._model_name}")

        self._worker.run(
            max_messages=1 if once else None
        )


class WorkerCLI:
    """
    Handles command-line arguments
    and starts the selected AI worker.
    """

    def __init__(self):

        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Worker"
        )

        self._parser.add_argument(
            "--model",
            required=True,
            choices=[
                "car",
                "fire",
                "person_face",
                "ppe",
            ],
            help="AI model to execute",
        )

        self._parser.add_argument(
            "--once",
            action="store_true",
            help="Handle one frame and exit",
        )

    def run(self) -> None:

        args = self._parser.parse_args()

        try:

            application = WorkerApplication(
                model_name=args.model
            )

            application.run(
                once=args.once
            )

        except KeyboardInterrupt:

            print("\nAI worker stopped by user.")


# ==========================================
# APPLICATION ENTRY POINT
# ==========================================

if __name__ == "__main__":
    WorkerCLI().run()