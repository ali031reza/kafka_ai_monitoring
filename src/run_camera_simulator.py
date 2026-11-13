
import argparse
from pathlib import Path

from config.config_manager import ConfigManager

from src.database.client import DatabaseClient
from src.database.camera_route_repository import (
    CameraRouteRepository,
)

from src.ingestion.frame_router import FrameRouter
from src.ingestion.frame_serializer import FrameSerializer
from src.ingestion.camera_simulator import VideoCameraSimulator

from src.kafka.frame_producer import KafkaFrameProducer


class CameraSimulatorApplication:
    """
    Initializes and runs a simulated camera.

    Responsibilities:
        - Load application configuration
        - Connect to PostgreSQL
        - Load camera routing assignments
        - Initialize Kafka frame producer
        - Initialize video camera simulator
        - Start continuous frame publishing
    """

    def __init__(
        self,
        camera_id: str,
        video_path: str,
        fps: float | None = None,
        loop: bool = True,
    ):

        # ==========================================
        # 1. PROJECT CONFIGURATION
        # ==========================================

        self._project_root = (
            Path(__file__).resolve().parents[1]
        )

        config = ConfigManager(
            self._project_root
        )

        kafka_config = config.get_section("kafka")

        camera_config = config.get_section("cameras")

        # ==========================================
        # 2. VALIDATE VIDEO FILE
        # ==========================================

        video_file = Path(video_path)

        if not video_file.is_absolute():
            video_file = (
                self._project_root / video_file
            )

        video_file = video_file.resolve()

        if not video_file.is_file():
            raise FileNotFoundError(
                f"Camera video not found: {video_file}"
            )

        # ==========================================
        # 3. INITIALIZE DATABASE
        # ==========================================

        database = DatabaseClient(
            **config.get_database_config()
        )

        repository = CameraRouteRepository(
            database
        )

        # ==========================================
        # 4. INITIALIZE FRAME ROUTER
        # ==========================================

        router = FrameRouter(
            repository
        )

        camera_count = router.refresh_routes()

        assigned_topics = router.get_topics(
            camera_id
        )

        if not assigned_topics:
            raise ValueError(
                f"Camera {camera_id} has no active "
                "model assignments in PostgreSQL"
            )

        print("\nCamera routing initialized.")

        print(
            f"Active cameras: {camera_count}"
        )

        print(
            f"Camera ID: {camera_id}"
        )

        print(
            f"Assigned Kafka topics: {assigned_topics}"
        )

        # ==========================================
        # 5. CAMERA SETTINGS
        # ==========================================

        resolution = camera_config["resolution"]

        target_fps = (
            fps
            if fps is not None
            else camera_config["fps"]
        )

        width = resolution["width"]
        height = resolution["height"]

        jpeg_quality = camera_config["jpeg_quality"]

        # ==========================================
        # 6. INITIALIZE KAFKA PRODUCER
        # ==========================================

        producer = KafkaFrameProducer(
            kafka_config=kafka_config,
            router=router,
            serializer=FrameSerializer(),
        )

        # ==========================================
        # 7. INITIALIZE CAMERA SIMULATOR
        # ==========================================

        self._simulator = VideoCameraSimulator(
            video_path=video_file,
            camera_id=camera_id,
            producer=producer,
            fps=target_fps,
            width=width,
            height=height,
            jpeg_quality=jpeg_quality,
            loop=loop,
            flush_every=max(1, round(target_fps)),
        )

        print("\nCamera simulator application initialized.")

    # ==========================================
    # 8. RUN SIMULATOR
    # ==========================================

    def run(
        self,
        max_frames: int | None = None,
    ) -> None:

        self._simulator.run(
            max_frames=max_frames
        )


class CameraSimulatorCLI:
    """Command-line interface for camera simulation."""

    def __init__(self):

        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Camera Simulator"
        )

        self._parser.add_argument(
            "--camera",
            required=True,
            help="Camera ID registered in PostgreSQL",
        )

        self._parser.add_argument(
            "--video",
            default="Camera_sample/TEST.mp4",
            help="Path to the sample MP4 video",
        )

        self._parser.add_argument(
            "--fps",
            type=float,
            default=None,
            help="Override configured camera FPS",
        )

        self._parser.add_argument(
            "--max-frames",
            type=int,
            default=None,
            help="Stop after publishing N frames",
        )

        self._parser.add_argument(
            "--no-loop",
            action="store_true",
            help="Stop when the video reaches EOF",
        )

    def run(self) -> None:

        args = self._parser.parse_args()

        try:

            application = CameraSimulatorApplication(
                camera_id=args.camera,
                video_path=args.video,
                fps=args.fps,
                loop=not args.no_loop,
            )

            application.run(
                max_frames=args.max_frames
            )

        except KeyboardInterrupt:

            print(
                "\nCamera simulator stopped by user."
            )


if __name__ == "__main__":
    CameraSimulatorCLI().run()
