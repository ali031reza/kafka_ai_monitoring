
import argparse
import threading
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


class MultiCameraSimulatorApplication:
    """
    Runs multiple video-based simulated cameras concurrently.

    Each camera:
        - Opens its own video capture
        - Loops the same MP4 independently
        - Generates unique frame IDs
        - Maintains its own sequence number
        - Uses its own Kafka producer
        - Publishes through the shared database-driven router

    All cameras share one PostgreSQL routing cache.
    """

    def __init__(
        self,
        count: int = 10,
        camera_ids: list[str] | None = None,
        video_path: str = "Camera_sample/TEST.mp4",
        fps: float | None = None,
        max_frames: int | None = None,
        loop: bool = True,
    ):

        # ==========================================
        # 1. LOAD CONFIGURATION
        # ==========================================

        self._root = Path(__file__).resolve().parents[1]

        config = ConfigManager(self._root)

        self._kafka_config = config.get_section("kafka")

        camera_config = config.get_section("cameras")

        self._width = camera_config["resolution"]["width"]
        self._height = camera_config["resolution"]["height"]

        self._jpeg_quality = camera_config["jpeg_quality"]

        self._fps = (
            fps if fps is not None else camera_config["fps"]
        )

        if self._fps <= 0:
            raise ValueError("FPS must be greater than zero")

        if count <= 0:
            raise ValueError("Camera count must be greater than zero")

        if max_frames is not None and max_frames <= 0:
            raise ValueError("max_frames must be greater than zero")

        self._max_frames = max_frames
        self._loop = loop

        # ==========================================
        # 2. VALIDATE VIDEO
        # ==========================================

        self._video_path = Path(video_path)

        if not self._video_path.is_absolute():
            self._video_path = self._root / self._video_path

        self._video_path = self._video_path.resolve()

        if not self._video_path.is_file():
            raise FileNotFoundError(
                f"Video not found: {self._video_path}"
            )

        # ==========================================
        # 3. CONNECT TO DATABASE
        # ==========================================

        database = DatabaseClient(
            **config.get_database_config()
        )

        repository = CameraRouteRepository(
            database
        )

        # ==========================================
        # 4. INITIALIZE SHARED ROUTER
        # ==========================================

        self._router = FrameRouter(repository)

        active_count = self._router.refresh_routes()

        routes = repository.get_all_routes()

        available_cameras = sorted(routes.keys())

        if camera_ids:

            selected = list(dict.fromkeys(camera_ids))

            if len(selected) != len(camera_ids):
                raise ValueError(
                    "Duplicate camera IDs were supplied"
                )

            missing = [
                camera_id
                for camera_id in selected
                if camera_id not in available_cameras
            ]

            if missing:
                raise ValueError(
                    f"Cameras are not actively assigned: {missing}"
                )

        else:

            if count > len(available_cameras):
                raise ValueError(
                    f"Requested {count} cameras, "
                    f"but only {len(available_cameras)} "
                    "active cameras are available"
                )

            selected = available_cameras[:count]

        self._camera_ids = selected

        # ==========================================
        # 5. THREAD MANAGEMENT
        # ==========================================

        self._stop_event = threading.Event()

        self._threads = []

        self._errors = []

        self._error_lock = threading.Lock()

        # ==========================================
        # 6. DISPLAY CONFIGURATION
        # ==========================================

        print("\n==========================================")
        print("MULTI-CAMERA SIMULATOR INITIALIZED")
        print("==========================================")

        print(f"Video: {self._video_path}")

        print(f"Active database cameras: {active_count}")

        print(f"Selected cameras: {len(self._camera_ids)}")

        print(f"Output FPS per camera: {self._fps}")

        print(
            f"Aggregate target FPS: "
            f"{len(self._camera_ids) * self._fps}"
        )

        print(
            f"Resolution: {self._width}x{self._height}"
        )

        print(f"JPEG quality: {self._jpeg_quality}")

        print("\nCamera routing assignments:")

        for camera_id in self._camera_ids:

            print(
                f"  {camera_id}: "
                f"{self._router.get_topics(camera_id)}"
            )

    # ==========================================
    # 7. RUN ONE CAMERA INDEPENDENTLY
    # ==========================================

    def _run_camera(self, camera_id: str) -> None:

        producer = None
        simulator = None

        try:

            print(
                f"\n[STARTING CAMERA] {camera_id}"
            )

            # Each camera owns its Kafka producer.
            producer = KafkaFrameProducer(
                kafka_config=self._kafka_config,
                router=self._router,
                serializer=FrameSerializer(),
            )

            # Open the video capture inside its own thread.
            simulator = VideoCameraSimulator(
                video_path=self._video_path,
                camera_id=camera_id,
                producer=producer,
                fps=self._fps,
                width=self._width,
                height=self._height,
                jpeg_quality=self._jpeg_quality,
                loop=self._loop,
                flush_every=max(1, round(self._fps)),
            )

            simulator.run(
                max_frames=self._max_frames,
                stop_event=self._stop_event,
            )

        except Exception as error:

            with self._error_lock:

                self._errors.append(
                    (camera_id, str(error))
                )

            print(
                f"\n[CAMERA ERROR] "
                f"Camera={camera_id}, "
                f"Error={error}"
            )

            # Stop all cameras if one fails.
            self._stop_event.set()

        finally:

            # If simulator initialization failed,
            # flush any producer that was already created.
            if producer is not None and simulator is None:

                try:
                    producer.flush(timeout=30)

                except Exception as error:

                    with self._error_lock:
                        self._errors.append(
                            (
                                camera_id,
                                f"Producer cleanup failed: {error}",
                            )
                        )

                    self._stop_event.set()

    # ==========================================
    # 8. START ALL CAMERAS
    # ==========================================

    def run(self) -> None:

        print("\nStarting all camera simulators...\n")

        for camera_id in self._camera_ids:

            thread = threading.Thread(
                target=self._run_camera,
                args=(camera_id,),
                name=f"camera-{camera_id}",
                daemon=False,
            )

            self._threads.append(thread)

        try:

            for thread in self._threads:
                thread.start()

            print(
                f"\n{len(self._threads)} camera threads started."
            )

            print("Press CTRL+C to stop all cameras.\n")

            while any(
                thread.is_alive()
                for thread in self._threads
            ):

                for thread in self._threads:
                    thread.join(timeout=0.5)

                if self._errors:
                    self._stop_event.set()

        except KeyboardInterrupt:

            print(
                "\n[STOP REQUESTED] "
                "Stopping all camera simulators..."
            )

            self._stop_event.set()

        finally:

            self._stop_event.set()

            for thread in self._threads:

                if thread.ident is not None:
                    thread.join()

        print("\n==========================================")
        print("MULTI-CAMERA SIMULATION FINISHED")
        print("==========================================")

        print(f"Cameras started: {len(self._threads)}")

        if self._errors:

            print("\nCamera errors:")

            for camera_id, error in self._errors:
                print(f"  {camera_id}: {error}")

            raise RuntimeError(
                "One or more camera simulators failed"
            )

        print("All camera simulators stopped successfully.")


class MultiCameraSimulatorCLI:

    def __init__(self):

        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Multi-Camera Simulator"
        )

        self._parser.add_argument(
            "--count",
            type=int,
            default=10,
            help="Number of active database cameras to simulate",
        )

        self._parser.add_argument(
            "--cameras",
            type=str,
            default=None,
            help="Explicit comma-separated camera IDs",
        )

        self._parser.add_argument(
            "--video",
            default="Camera_sample/TEST.mp4",
            help="MP4 video used by all cameras",
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
            help="Maximum frames per camera",
        )

        self._parser.add_argument(
            "--no-loop",
            action="store_true",
            help="Stop when each video reaches EOF",
        )

    def run(self) -> None:

        args = self._parser.parse_args()

        camera_ids = None

        if args.cameras:

            camera_ids = [
                camera.strip()
                for camera in args.cameras.split(",")
                if camera.strip()
            ]

        application = MultiCameraSimulatorApplication(
            count=args.count,
            camera_ids=camera_ids,
            video_path=args.video,
            fps=args.fps,
            max_frames=args.max_frames,
            loop=not args.no_loop,
        )

        application.run()


if __name__ == "__main__":
    MultiCameraSimulatorCLI().run()
