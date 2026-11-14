"""Convert project-level inference outcomes to the existing Protobuf contract.

DeepStream-specific metadata extraction will feed this adapter in a later
phase. The current InferenceResult schema does not contain detection boxes,
classes, or confidence values.
"""

import time

from src.proto.inference_result_pb2 import InferenceResult
from src.storage.event_image_storage import StoredEventImage


def build_inference_result(
    *,
    camera_id: str,
    frame_id: str,
    model_id: str,
    captured_at_ms: int,
    status: str,
    event_type: str | None = None,
    event_image: StoredEventImage | None = None,
    processed_at_ms: int | None = None,
) -> InferenceResult:
    """Build a validated InferenceResult using the existing project schema.

    Event results must reference an image already written by EventImageStorage,
    because the current ResultsProcessingService requires an image for events.
    """
    if not camera_id or len(camera_id) > 50:
        raise ValueError("Invalid camera_id")
    if not frame_id or len(frame_id) > 100:
        raise ValueError("Invalid frame_id")
    if not model_id or len(model_id) > 50:
        raise ValueError("Invalid model_id")
    if not status or len(status) > 50:
        raise ValueError("Invalid result status")
    if captured_at_ms <= 0:
        raise ValueError("captured_at_ms must be positive")

    if processed_at_ms is None:
        processed_at_ms = time.time_ns() // 1_000_000
    if processed_at_ms <= 0:
        raise ValueError("processed_at_ms must be positive")

    is_event = event_type is not None
    if is_event and (not event_type or len(event_type) > 100):
        raise ValueError("Event results require a valid event_type")
    if is_event and event_image is None:
        raise ValueError("Event results require a stored event image")
    if not is_event and event_image is not None:
        raise ValueError("Non-event results cannot reference an event image")

    if event_image is not None:
        if event_image.captured_at_ms != captured_at_ms:
            raise ValueError("Event image capture timestamp does not match result")
        if not event_image.storage_key or not event_image.sha256:
            raise ValueError("Stored event image metadata is incomplete")

    result = InferenceResult(
        camera_id=camera_id,
        frame_id=frame_id,
        model_id=model_id,
        processed_at_ms=processed_at_ms,
        status=status,
        is_event=is_event,
        captured_at_ms=captured_at_ms,
    )

    if event_image is not None:
        result.event_type = event_type
        result.storage_key = event_image.storage_key
        result.image_format = event_image.image_format
        result.image_size_bytes = event_image.file_size_bytes
        result.image_sha256 = event_image.sha256

    return result
