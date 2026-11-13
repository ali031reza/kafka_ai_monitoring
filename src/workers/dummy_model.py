import os
import time

from src.workers.base_model import BaseAIModel

from src.proto.camera_frame_pb2 import CameraFrame
from src.proto.inference_result_pb2 import InferenceResult


class DummyAIModel(BaseAIModel):
    """
    Dummy AI model for development and testing.

    Supports:
        - Simulated inference failures
        - Simulated meaningful events
        - Four different model types
    """

    EVENT_TYPES = {
        "car": "vehicle_detected",
        "fire": "fire_detected",
        "person_face": "person_detected",
        "ppe": "ppe_violation",
    }

    def __init__(self, model_id: str):

        self._model_id = model_id

        self._simulate_failure = (
            os.getenv("SIMULATE_AI_FAILURE", "0") == "1"
        )

        self._simulate_event = (
            os.getenv("SIMULATE_EVENT", "0") == "1"
        )

    def predict(self, frame: CameraFrame) -> InferenceResult:
        """Simulate AI inference and return result metadata."""

        if self._simulate_failure:
            raise RuntimeError(
                "Simulated AI inference failure"
            )

        print(
            f"[{self._model_id}] Processing "
            f"camera={frame.camera_id}, "
            f"frame={frame.frame_id}"
        )

        result = InferenceResult(
            camera_id=frame.camera_id,
            frame_id=frame.frame_id,
            model_id=self._model_id,
            processed_at_ms=time.time_ns() // 1_000_000,
            status="dummy_processed",
            is_event=self._simulate_event,
        )

        if self._simulate_event:
            result.event_type = self.EVENT_TYPES[
                self._model_id
            ]

            print(
                f"[EVENT] {self._model_id}: "
                f"{result.event_type}"
            )

        return result