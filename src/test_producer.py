import argparse
import time

from io import BytesIO
from pathlib import Path
from uuid import uuid4

from PIL import Image

from config.config_manager import ConfigManager

from src.database.client import DatabaseClient
from src.database.camera_route_repository import CameraRouteRepository

from src.ingestion.frame_router import FrameRouter
from src.ingestion.frame_serializer import FrameSerializer

from src.kafka.frame_producer import KafkaFrameProducer


class ProducerSmokeTest:
    """
    Publishes test camera frames through the normal
    database-driven Kafka routing architecture.

    Responsibilities:
        - Generate test JPEG images
        - Generate unique frame IDs
        - Retrieve camera routing information
        - Publish frames to assigned AI models
    """

    def __init__(self, camera_id: str):

        self._camera_id = camera_id

        # 1. Locate project root
        root = Path(__file__).resolve().parents[1]

        # 2. Load configuration
        config = ConfigManager(root)

        # 3. Initialize database
        database = DatabaseClient(
            **config.get_database_config()
        )

        # 4. Initialize camera routing repository
        repository = CameraRouteRepository(database)

        # 5. Initialize frame router
        router = FrameRouter(repository)

        router.refresh_routes()

        # 6. Initialize Kafka producer
        self._producer = KafkaFrameProducer(
            kafka_config=config.get_section("kafka"),
            router=router,
            serializer=FrameSerializer(),
        )

    def _generate_test_image(self) -> bytes:
        """Generate a valid JPEG test image."""

        image = Image.new(
            "RGB",
            (1920, 1080),
            (100, 120, 140),
        )

        buffer = BytesIO()

        image.save(
            buffer,
            format="JPEG",
            quality=80,
        )

        return buffer.getvalue()

    def run(self) -> None:
        """Generate and publish one test frame."""

        frame_id = str(uuid4())

        timestamp_ms = time.time_ns() // 1_000_000

        image_data = self._generate_test_image()

        print("\nPreparing test frame...")
        print(f"Camera ID: {self._camera_id}")
        print(f"Frame ID: {frame_id}")
        print(f"Image size: {len(image_data)} bytes")

        # Publish using database-driven routing
        count = self._producer.publish_frame(
            camera_id=self._camera_id,
            frame_id=frame_id,
            timestamp_ms=timestamp_ms,
            sequence_number=1,
            image_data=image_data,
        )

        print(f"\nQueued {count} messages")

        # Wait for Kafka delivery confirmations
        self._producer.flush()

        print("\nFrame publishing completed.")


class ProducerCLI:
    """Command-line interface for the test producer."""

    def __init__(self):

        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Test Producer"
        )

        self._parser.add_argument(
            "--camera",
            required=True,
            help="Camera ID to publish a test frame from",
        )

    def run(self) -> None:

        args = self._parser.parse_args()

        application = ProducerSmokeTest(
            camera_id=args.camera
        )

        application.run()


if __name__ == "__main__":
    ProducerCLI().run()