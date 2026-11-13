
import time

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
    """
    Kafka AI Worker with event-based image storage.

    Responsibilities:
        - Consume camera frames from Kafka
        - Deserialize Protobuf messages
        - Validate incoming frames
        - Skip stale frames
        - Execute AI inference
        - Retry failed inference
        - Detect meaningful events
        - Save full JPEG images for events
        - Attach image metadata to inference results
        - Publish inference results to Kafka
        - Publish failed messages to DLT
        - Commit Kafka offsets safely

    The worker does not communicate directly with PostgreSQL.
    Database persistence is handled by ResultsProcessingService.
    """

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
    ):

        if not model_id:
            raise ValueError("model_id cannot be empty")

        if not result_topic:
            raise ValueError("result_topic cannot be empty")

        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if retry_delay_ms < 0:
            raise ValueError("retry_delay_ms cannot be negative")

        # Kafka dependencies
        self._consumer = consumer
        self._producer = producer

        # Serialization
        self._serializer = serializer

        # AI model
        self._model = model
        self._model_id = model_id

        # Kafka result destination
        self._result_topic = result_topic

        # Processing policies
        self._freshness_policy = freshness_policy

        # Failure handling
        self._dead_letter = dead_letter_publisher

        # Event image storage
        self._image_storage = image_storage

        # Retry configuration
        self._max_retries = max_retries
        self._retry_delay_ms = retry_delay_ms

    # ==========================================
    # 1. FRAME VALIDATION
    # ==========================================

    def _validate_frame(self, frame: CameraFrame) -> None:
        """
        Validate incoming camera frame.

        Raises:
            ValueError: If frame metadata or image is invalid.
        """

        if not frame.camera_id:
            raise ValueError("Missing camera_id")

        if len(frame.camera_id) > 50:
            raise ValueError("camera_id is too long")

        if not frame.frame_id:
            raise ValueError("Missing frame_id")

        if len(frame.frame_id) > 100:
            raise ValueError("frame_id is too long")

        if frame.timestamp_ms <= 0:
            raise ValueError("Invalid frame timestamp")

        if frame.image_format.lower() not in ("jpeg", "jpg"):
            raise ValueError("Unsupported image format")

        if not frame.image_data:
            raise ValueError("Empty image data")

        # Validate JPEG magic bytes.
        if not frame.image_data.startswith(b"\xff\xd8"):
            raise ValueError("Invalid JPEG header")

    # ==========================================
    # 2. INFERENCE RESULT VALIDATION
    # ==========================================

    def _validate_result(
        self,
        frame: CameraFrame,
        result: InferenceResult,
    ) -> None:
        """
        Validate inference result against its source frame.
        """

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
            raise ValueError("Missing inference status")

        if result.is_event and not result.event_type:
            raise ValueError(
                "Event result must contain event_type"
            )

    # ==========================================
    # 3. EVENT IMAGE STORAGE
    # ==========================================

    def _store_event_image(
        self,
        frame: CameraFrame,
        result: InferenceResult,
    ) -> None:
        """
        Save the complete JPEG frame when a meaningful
        event is detected.

        Non-event results do not generate image files.
        """

        if not result.is_event:

            result.storage_key = ""
            result.image_format = ""
            result.image_size_bytes = 0
            result.image_sha256 = ""
            result.captured_at_ms = 0

            return

        print(
            f"[EVENT] {self._model_id}: "
            f"{result.event_type}"
        )

        # Save original full-resolution JPEG.
        stored_image = self._image_storage.save_full_frame(
            camera_id=frame.camera_id,
            frame_id=frame.frame_id,
            captured_at_ms=frame.timestamp_ms,
            image_format=frame.image_format,
            image_data=frame.image_data,
        )

        # Attach image metadata to Protobuf result.
        result.storage_key = stored_image.storage_key

        result.image_format = stored_image.image_format

        result.image_size_bytes = (
            stored_image.file_size_bytes
        )

        result.image_sha256 = stored_image.sha256

        # Preserve original camera capture timestamp.
        result.captured_at_ms = frame.timestamp_ms

        print(
            f"[EVENT IMAGE SAVED] "
            f"camera={frame.camera_id}, "
            f"frame={frame.frame_id}, "
            f"event={result.event_type}, "
            f"path={stored_image.storage_key}"
        )

    # ==========================================
    # 4. FAILURE HANDLING
    # ==========================================

    def _publish_failure(
        self,
        message,
        error: Exception,
        frame: CameraFrame | None = None,
        attempts: int = 1,
    ) -> None:
        """
        Publish failed message to Dead Letter Topic.

        The original Kafka offset is committed only
        after successful DLT publication.
        """

        self._dead_letter.publish_failure(
            message=message,
            error=error,
            frame=frame,
            attempts=attempts,
        )

        self._consumer.commit(message)

        print(
            f"[DLT] Published failure "
            f"model={self._model_id}, "
            f"attempts={attempts}"
        )

    # ==========================================
    # 5. PROCESS ONE KAFKA MESSAGE
    # ==========================================

    def _handle_message(self, message) -> str:
        """
        Process one incoming Kafka frame.

        Returns:
            processed
            skipped
            failed
        """

        frame = None

        # --------------------------------------
        # STEP 1: Deserialize and validate frame
        # --------------------------------------

        try:

            frame = self._serializer.deserialize(
                message.value()
            )

            self._validate_frame(frame)

        except (DecodeError, ValueError, TypeError) as error:

            self._publish_failure(
                message=message,
                error=error,
                frame=frame,
                attempts=1,
            )

            return "failed"

        # --------------------------------------
        # STEP 2: Check frame freshness
        # --------------------------------------

        is_stale, age_ms = self._freshness_policy.evaluate(
            frame.timestamp_ms
        )

        if is_stale:

            print(
                f"[SKIPPED] Camera={frame.camera_id}, "
                f"age={age_ms}ms"
            )

            self._consumer.commit(message)

            return "skipped"

        # --------------------------------------
        # STEP 3: Execute inference with retries
        # --------------------------------------

        result = None

        for attempt in range(1, self._max_retries + 2):

            # Recheck freshness before inference.
            is_stale, age_ms = self._freshness_policy.evaluate(
                frame.timestamp_ms
            )

            if is_stale:

                print(
                    f"[SKIPPED] Camera={frame.camera_id}: "
                    "frame expired during retries"
                )

                self._consumer.commit(message)

                return "skipped"

            try:

                # Run AI inference.
                result = self._model.predict(frame)

                # Validate inference result.
                self._validate_result(frame, result)

                # Save image only for meaningful events.
                self._store_event_image(frame, result)

                # Successful inference and image handling.
                break

            except Exception as error:

                # All attempts exhausted.
                if attempt > self._max_retries:

                    print(
                        f"[FAILED] Camera={frame.camera_id}: "
                        "all attempts exhausted"
                    )

                    self._publish_failure(
                        message=message,
                        error=error,
                        frame=frame,
                        attempts=attempt,
                    )

                    return "failed"

                # Retry.
                print(
                    f"[RETRY] Camera={frame.camera_id}: "
                    f"attempt {attempt}/{self._max_retries}"
                )

                time.sleep(
                    self._retry_delay_ms / 1000
                )

        # --------------------------------------
        # STEP 4: Publish inference result
        # --------------------------------------

        # Result includes event image metadata when applicable.
        #
        # The producer must report delivery failure by
        # raising an exception. Do not commit input if
        # result delivery has not succeeded.

        self._producer.publish(
            self._result_topic,
            result,
        )

        # --------------------------------------
        # STEP 5: Commit original input offset
        # --------------------------------------

        self._consumer.commit(message)

        print(
            f"[PROCESSED] "
            f"Camera={frame.camera_id}, "
            f"Model={self._model_id}, "
            f"Event={result.is_event}, "
            f"Offset={message.offset()}"
        )

        return "processed"

    # ==========================================
    # 6. MAIN WORKER LOOP
    # ==========================================

    def run(
        self,
        max_messages: int | None = None,
    ) -> None:
        """
        Continuously consume Kafka messages.

        Args:
            max_messages:
                None -> Run indefinitely.
                1    -> Process one message and exit.
        """

        counters = {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
        }

        handled = 0

        print(
            f"AI worker started for model: {self._model_id}"
        )

        print("Waiting for frames...")

        try:

            while (
                max_messages is None
                or handled < max_messages
            ):

                # Poll Kafka.
                message = self._consumer.poll(
                    timeout=1.0
                )

                if message is None:
                    continue

                # Handle Kafka-level errors.
                if message.error():

                    raise RuntimeError(
                        f"Kafka consumer error: {message.error()}"
                    )

                # Process message.
                outcome = self._handle_message(
                    message
                )

                counters[outcome] += 1

                handled += 1

        finally:

            # Release Kafka consumer resources.
            self._consumer.close()

            print("\nWorker stopped.")

            print(
                f"Processed={counters['processed']}, "
                f"Skipped={counters['skipped']}, "
                f"Failed={counters['failed']}"
            )
