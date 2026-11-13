
import time
import threading
from pathlib import Path
from uuid import uuid4

import cv2
import numpy as np

from src.kafka.frame_producer import KafkaFrameProducer


class VideoCameraSimulator:
    """
    Simulates a live IP camera using a looping MP4 video.

    Features:
        - Independent video capture per camera
        - Automatic video looping
        - Configurable FPS sampling
        - Configurable resolution
        - Aspect-ratio preservation
        - JPEG encoding
        - Unique frame IDs
        - Sequence numbering
        - Kafka frame publishing
        - Periodic Kafka delivery flushing
        - Graceful shutdown using threading.Event
        - Maximum frame limit for testing
    """

    def __init__(
        self,
        video_path: Path,
        camera_id: str,
        producer: KafkaFrameProducer,
        fps: float = 12.0,
        width: int = 1920,
        height: int = 1080,
        jpeg_quality: int = 80,
        loop: bool = True,
        flush_every: int = 12,
    ):

        # ==========================================
        # 1. VALIDATE CONFIGURATION
        # ==========================================

        self._video_path = Path(video_path)

        if not self._video_path.is_file():
            raise FileNotFoundError(
                f"Video file not found: {self._video_path}"
            )

        if not camera_id:
            raise ValueError("Camera ID cannot be empty")

        if fps <= 0:
            raise ValueError("FPS must be greater than zero")

        if width <= 0 or height <= 0:
            raise ValueError("Invalid camera resolution")

        if not 0 <= jpeg_quality <= 100:
            raise ValueError(
                "JPEG quality must be between 0 and 100"
            )

        if flush_every <= 0:
            raise ValueError(
                "flush_every must be greater than zero"
            )

        self._camera_id = camera_id
        self._producer = producer

        self._requested_fps = float(fps)

        self._width = width
        self._height = height

        self._jpeg_quality = jpeg_quality

        self._loop = loop
        self._flush_every = flush_every

        # ==========================================
        # 2. INITIALIZE COUNTERS
        # ==========================================

        self._sequence_number = 0

        self._published_frames = 0

        self._queued_messages = 0

        self._source_frame_index = 0

        self._next_source_sample_time = 0.0

        # ==========================================
        # 3. OPEN VIDEO
        # ==========================================

        self._capture = cv2.VideoCapture(
            str(self._video_path)
        )

        if not self._capture.isOpened():

            self._capture.release()

            raise RuntimeError(
                f"Could not open video: {self._video_path}"
            )

        # ==========================================
        # 4. READ VIDEO METADATA
        # ==========================================

        self._source_fps = self._capture.get(
            cv2.CAP_PROP_FPS
        )

        if (
            not np.isfinite(self._source_fps)
            or self._source_fps <= 0
        ):

            self._source_fps = self._requested_fps

            print(
                "[WARNING] Source FPS unavailable. "
                "Using configured FPS."
            )

        # Do not play the source faster than its native FPS.
        self._output_fps = min(
            self._requested_fps,
            self._source_fps,
        )

        self._frame_interval = (
            1.0 / self._output_fps
        )

        if self._source_fps < self._requested_fps:

            print(
                f"[WARNING] Camera={camera_id}: "
                f"Source FPS ({self._source_fps}) "
                f"is lower than requested FPS "
                f"({self._requested_fps}). "
                f"Output will be limited to source FPS."
            )

        # ==========================================
        # 5. DISPLAY INITIALIZATION
        # ==========================================

        print("\n==========================================")
        print("VIDEO CAMERA SIMULATOR INITIALIZED")
        print("==========================================")

        print(f"Camera ID: {self._camera_id}")

        print(f"Video: {self._video_path}")

        print(
            f"Source FPS: {self._source_fps:.2f}"
        )

        print(
            f"Output FPS: {self._output_fps:.2f}"
        )

        print(
            f"Resolution: "
            f"{self._width}x{self._height}"
        )

        print(
            f"JPEG quality: {self._jpeg_quality}"
        )

        print(f"Loop enabled: {self._loop}")

    # ==========================================
    # 6. RESIZE FRAME
    # ==========================================

    def _prepare_frame(self, frame):
        """
        Resize frame while preserving aspect ratio.

        Black padding is added where necessary.
        """

        original_height, original_width = (
            frame.shape[:2]
        )

        scale = min(
            self._width / original_width,
            self._height / original_height,
        )

        resized_width = max(
            1,
            int(round(original_width * scale)),
        )

        resized_height = max(
            1,
            int(round(original_height * scale)),
        )

        resized = cv2.resize(
            frame,
            (resized_width, resized_height),
            interpolation=(
                cv2.INTER_AREA
                if scale < 1
                else cv2.INTER_LINEAR
            ),
        )

        canvas = np.zeros(
            (self._height, self._width, 3),
            dtype=np.uint8,
        )

        x_offset = (
            self._width - resized_width
        ) // 2

        y_offset = (
            self._height - resized_height
        ) // 2

        canvas[
            y_offset:y_offset + resized_height,
            x_offset:x_offset + resized_width,
        ] = resized

        return canvas

    # ==========================================
    # 7. JPEG ENCODING
    # ==========================================

    def _encode_jpeg(self, frame) -> bytes:
        """
        Encode an OpenCV frame as JPEG bytes.
        """

        success, encoded = cv2.imencode(
            ".jpg",
            frame,
            [
                cv2.IMWRITE_JPEG_QUALITY,
                self._jpeg_quality,
            ],
        )

        if not success:

            raise RuntimeError(
                f"JPEG encoding failed for camera "
                f"{self._camera_id}"
            )

        return encoded.tobytes()

    # ==========================================
    # 8. RESTART VIDEO
    # ==========================================

    def _restart_video(self) -> bool:
        """
        Restart video playback after reaching EOF.

        Returns:
            True if video restarted successfully.
            False if looping is disabled.
        """

        if not self._loop:
            return False

        print(
            f"\n[VIDEO LOOP] "
            f"Camera={self._camera_id} "
            f"Restarting TEST video..."
        )

        self._capture.set(
            cv2.CAP_PROP_POS_FRAMES,
            0,
        )

        self._source_frame_index = 0

        self._next_source_sample_time = 0.0

        success, frame = self._capture.read()

        if not success:

            raise RuntimeError(
                f"Unable to restart video for "
                f"camera {self._camera_id}"
            )

        self._pending_frame = frame

        return True

    # ==========================================
    # 9. READ NEXT SAMPLED FRAME
    # ==========================================

    def _read_next_sampled_frame(self, stop_event):
        """
        Read source frames and select frames at
        the configured output FPS.

        Returns:
            Selected frame or None.
        """

        while not stop_event.is_set():

            if hasattr(self, "_pending_frame"):

                frame = self._pending_frame

                del self._pending_frame

                success = True

            else:

                success, frame = self._capture.read()

            if not success:

                restarted = self._restart_video()

                if not restarted:
                    return None

                continue

            source_time = (
                self._source_frame_index
                / self._source_fps
            )

            self._source_frame_index += 1

            # Skip frames outside the sampling interval.
            if (
                source_time + 1e-9
                < self._next_source_sample_time
            ):

                continue

            # Advance sampling schedule.
            while (
                self._next_source_sample_time
                <= source_time + 1e-9
            ):

                self._next_source_sample_time += (
                    self._frame_interval
                )

            return frame

        return None

    # ==========================================
    # 10. FRAME PACING
    # ==========================================

    def _wait_for_next_frame(
        self,
        next_publish_at: float,
        stop_event,
    ):
        """
        Maintain the configured camera FPS.

        Event.wait() allows immediate interruption
        when the multi-camera manager stops.
        """

        now = time.monotonic()

        delay = next_publish_at - now

        if delay > 0:

            if stop_event.wait(delay):
                return None

        else:

            # Do not publish accumulated frames in bursts.
            if abs(delay) > self._frame_interval:

                next_publish_at = time.monotonic()

        return next_publish_at

    # ==========================================
    # 11. PUBLISH ONE FRAME
    # ==========================================

    def _publish_frame(self, frame) -> int:
        """
        Prepare, encode and publish a camera frame.
        """

        prepared_frame = self._prepare_frame(
            frame
        )

        image_data = self._encode_jpeg(
            prepared_frame
        )

        self._sequence_number += 1

        frame_id = str(uuid4())

        timestamp_ms = (
            time.time_ns() // 1_000_000
        )

        queued = self._producer.publish_frame(
            camera_id=self._camera_id,
            frame_id=frame_id,
            timestamp_ms=timestamp_ms,
            sequence_number=self._sequence_number,
            image_data=image_data,
        )

        self._published_frames += 1

        self._queued_messages += queued

        return queued

    # ==========================================
    # 12. MAIN CAMERA LOOP
    # ==========================================

    def run(
        self,
        max_frames: int | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        """
        Run the simulated camera.

        Args:
            max_frames:
                None -> Run continuously.
                N    -> Stop after N published frames.

            stop_event:
                Shared event used by the multi-camera
                manager to stop all camera threads.
        """

        if (
            max_frames is not None
            and max_frames <= 0
        ):

            raise ValueError(
                "max_frames must be greater than zero"
            )

        if stop_event is None:

            stop_event = threading.Event()

        print(
            f"\n[CAMERA STARTED] "
            f"Camera={self._camera_id}"
        )

        print(
            f"Target FPS={self._output_fps:.2f}"
        )

        run_started_at = time.monotonic()
        next_publish_at = run_started_at

        queued_since_flush = 0

        last_report = time.monotonic()

        try:

            while not stop_event.is_set():

                # Check frame limit.
                if (
                    max_frames is not None
                    and self._published_frames >= max_frames
                ):

                    break

                # Read next sampled video frame.
                frame = self._read_next_sampled_frame(
                    stop_event
                )

                if frame is None:
                    break

                # Maintain output FPS.
                next_publish_at = self._wait_for_next_frame(
                    next_publish_at,
                    stop_event,
                )

                if next_publish_at is None:
                    break

                if stop_event.is_set():
                    break

                # Publish to Kafka.
                queued = self._publish_frame(
                    frame
                )

                queued_since_flush += queued

                next_publish_at += (
                    self._frame_interval
                )

                # Report progress once per second.
                now = time.monotonic()

                if now - last_report >= 1.0:

                    print(
                        f"[CAMERA] "
                        f"ID={self._camera_id}, "
                        f"Frames={self._published_frames}, "
                        f"Sequence={self._sequence_number}, "
                        f"Messages={self._queued_messages}"
                    )

                    last_report = now

                # Periodic Kafka delivery confirmation.
                if (
                    queued_since_flush
                    >= self._flush_every
                ):

                    self._producer.flush(
                        timeout=10.0
                    )

                    queued_since_flush = 0

        finally:

            elapsed_seconds = max(
                time.monotonic() - run_started_at,
                1e-9,
            )

            # ======================================
            # 13. CLEANUP
            # ======================================

            self._capture.release()

            print(
                f"\n[CAMERA STOPPING] "
                f"Camera={self._camera_id}"
            )

            # Flush outstanding Kafka messages.
            self._producer.flush(
                timeout=30.0
            )

            print("\n==========================================")

            print(
                f"CAMERA {self._camera_id} SUMMARY"
            )

            print("==========================================")

            print(
                f"Published frames: "
                f"{self._published_frames}"
            )

            print(
                f"Kafka messages queued: "
                f"{self._queued_messages}"
            )

            print(
                f"Run elapsed seconds: "
                f"{elapsed_seconds:.3f}"
            )

            print(
                f"Actual camera FPS: "
                f"{self._published_frames / elapsed_seconds:.3f}"
            )

            print(
                f"Final sequence: "
                f"{self._sequence_number}"
            )

            print("==========================================")
