import time

import cv2
import numpy as np
from google.protobuf.message import DecodeError

from src.proto.camera_frame_pb2 import CameraFrame
from src.proto.inference_result_pb2 import InferenceResult

from src.workers.base_model import BaseAIModel
from src.workers.freshness_policy import FrameFreshnessPolicy
from src.workers.dead_letter_publisher import DeadLetterPublisher

from src.kafka.frame_consumer import KafkaFrameConsumer
from src.kafka.result_producer import KafkaResultProducer

from src.ingestion.frame_serializer import FrameSerializer
from src.storage.event_image_storage import EventImageStorage


class AIWorker:
    """Consumes frames, decodes JPEG, runs inference, and publishes results."""

    def __init__(
        self,
        consumer: KafkaFrameConsumer,
        producer: KafkaResultProducer,
        serializer: FrameSerializer,
        model: BaseAIModel,
        model_id: str,
        result_topic: str,
        freshness_policy: FrameFreshnessPolicy,
        dead_letter_publisher: DeadLetterPublisher,
        image_storage: EventImageStorage,
        max_retries: int = 2,
        retry_delay_ms: int = 200,
        progress_log_every: int = 100,
    ):
        if not model_id or not result_topic:
            raise ValueError("model_id and result_topic are required")
        if max_retries < 0 or retry_delay_ms < 0:
            raise ValueError("Invalid retry configuration")
        if progress_log_every <= 0:
            raise ValueError("progress_log_every must be positive")

        self._consumer = consumer
        self._producer = producer
        self._serializer = serializer
        self._model = model
        self._model_id = model_id
        self._result_topic = result_topic
        self._freshness_policy = freshness_policy
        self._dead_letter = dead_letter_publisher
        self._image_storage = image_storage
        self._max_retries = max_retries
        self._retry_delay_ms = retry_delay_ms
        self._progress_log_every = progress_log_every
        self._decoded_frames = 0

    def _validate_frame(self, frame: CameraFrame) -> None:
        if not frame.camera_id or len(frame.camera_id) > 50:
            raise ValueError("Invalid or missing camera_id")
        if not frame.frame_id or len(frame.frame_id) > 100:
            raise ValueError("Invalid or missing frame_id")
        if frame.timestamp_ms <= 0:
            raise ValueError("Invalid frame timestamp")
        if frame.image_format.lower() not in ("jpeg", "jpg"):
            raise ValueError("Unsupported image format")
        if not frame.image_data:
            raise ValueError("Empty image data")
        if not frame.image_data.startswith(b"\xff\xd8"):
            raise ValueError("Invalid JPEG header")

    def _decode_jpeg(self, frame: CameraFrame) -> np.ndarray:
        """Decode Kafka JPEG bytes directly into an in-memory BGR array."""
        encoded = np.frombuffer(frame.image_data, dtype=np.uint8)
        try:
            image = cv2.imdecode(encoded, cv2.IMREAD_COLOR)
        except cv2.error as error:
            raise ValueError(
                f"OpenCV JPEG decoding failed: {error}"
            ) from error

        if image is None:
            raise ValueError("JPEG could not be decoded")
        if image.ndim != 3 or image.shape[2] != 3:
            raise ValueError(
                f"Expected three-channel BGR image, got {image.shape}"
            )

        height, width = image.shape[:2]
        if height <= 0 or width <= 0:
            raise ValueError("Decoded image has invalid dimensions")

        self._decoded_frames += 1
        return image

    def _validate_result(
        self,
        frame: CameraFrame,
        result: InferenceResult,
    ) -> None:
        if result is None:
            raise ValueError("AI model returned no result")
        if result.camera_id != frame.camera_id:
            raise ValueError("Result camera_id mismatch")
        if result.frame_id != frame.frame_id:
            raise ValueError("Result frame_id mismatch")
        if result.model_id != self._model_id:
            raise ValueError("Result model_id mismatch")
        if result.processed_at_ms <= 0:
            raise ValueError("Invalid processing timestamp")
        if not result.status:
            raise ValueError("Missing result status")
        if result.is_event and not result.event_type:
            raise ValueError("Event result is missing event_type")

    def _store_event_image(
        self,
        frame: CameraFrame,
        result: InferenceResult,
    ) -> None:
        if not result.is_event:
            result.storage_key = ""
            result.image_format = ""
            result.image_size_bytes = 0
            result.image_sha256 = ""
            result.captured_at_ms = 0
            return

        # Store the original JPEG bytes; do not re-encode the decoded image.
        stored = self._image_storage.save_full_frame(
            camera_id=frame.camera_id,
            frame_id=frame.frame_id,
            captured_at_ms=frame.timestamp_ms,
            image_format=frame.image_format,
            image_data=frame.image_data,
        )

        result.storage_key = stored.storage_key
        result.image_format = stored.image_format
        result.image_size_bytes = stored.file_size_bytes
        result.image_sha256 = stored.sha256
        result.captured_at_ms = frame.timestamp_ms

    def _publish_failure(
        self,
        message,
        error: Exception,
        frame: CameraFrame | None,
        attempts: int,
    ) -> str:
        # The source offset is committed only after DLT delivery succeeds.
        self._dead_letter.publish_failure(
            message=message,
            error=error,
            frame=frame,
            attempts=attempts,
        )
        self._consumer.commit(message)
        return "failed"

    def _check_freshness(self, message, frame):
        try:
            return self._freshness_policy.evaluate(frame.timestamp_ms)
        except ValueError as error:
            self._publish_failure(
                message=message,
                error=error,
                frame=frame,
                attempts=1,
            )
            return None

    def _handle_message(self, message) -> str:
        frame = None

        # 1. Deserialize and validate metadata.
        try:
            frame = self._serializer.deserialize(message.value())
            self._validate_frame(frame)
        except (DecodeError, ValueError, TypeError) as error:
            return self._publish_failure(
                message, error, frame, attempts=1
            )

        # 2. Reject stale frames before spending CPU on JPEG decoding.
        freshness = self._check_freshness(message, frame)
        if freshness is None:
            return "failed"
        is_stale, _ = freshness
        if is_stale:
            self._consumer.commit(message)
            return "skipped"

        # 3. Decode once and reuse the image for inference retries.
        try:
            image = self._decode_jpeg(frame)
        except ValueError as error:
            return self._publish_failure(
                message, error, frame, attempts=1
            )

        # 4. Run inference, retrying temporary failures.
        result = None
        for attempt in range(1, self._max_retries + 2):
            freshness = self._check_freshness(message, frame)
            if freshness is None:
                return "failed"

            is_stale, _ = freshness
            if is_stale:
                self._consumer.commit(message)
                return "skipped"

            try:
                result = self._model.predict(frame, image)
                self._validate_result(frame, result)
                self._store_event_image(frame, result)
                break
            except Exception as error:
                if attempt > self._max_retries:
                    return self._publish_failure(
                        message, error, frame, attempts=attempt
                    )
                time.sleep(self._retry_delay_ms / 1000.0)

        if result is None:
            raise RuntimeError("Inference completed without a result")

        # 5. Confirm result delivery before committing the source message.
        self._producer.publish(self._result_topic, result)
        self._consumer.commit(message)
        return "processed"

    def run(self, max_messages: int | None = None, stop_event=None) -> None:
        counters = {"processed": 0, "skipped": 0, "failed": 0}
        handled = 0

        print(
            f"[AI WORKER STARTED] model={self._model_id}; "
            "JPEG decoding enabled"
        )

        try:
            while (
                max_messages is None or handled < max_messages
            ) and (
                stop_event is None or not stop_event.is_set()
            ):
                message = self._consumer.poll(timeout=0.5)
                if message is None:
                    continue

                outcome = self._handle_message(message)
                counters[outcome] += 1
                handled += 1

                if handled % self._progress_log_every == 0:
                    print(
                        f"[WORKER PROGRESS] model={self._model_id}, "
                        f"handled={handled}, processed={counters['processed']}, "
                        f"skipped={counters['skipped']}, failed={counters['failed']}, "
                        f"jpeg_decoded={self._decoded_frames}"
                    )
        finally:
            self._consumer.close()
            print(
                f"[AI WORKER STOPPED] model={self._model_id}, "
                f"processed={counters['processed']}, "
                f"skipped={counters['skipped']}, failed={counters['failed']}, "
                f"jpeg_decoded={self._decoded_frames}"
            )
