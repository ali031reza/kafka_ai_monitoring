import os
import random
import time

import numpy as np

from src.workers.base_model import BaseAIModel
from src.proto.camera_frame_pb2 import CameraFrame
from src.proto.inference_result_pb2 import InferenceResult


class DummyAIModel(BaseAIModel):
    """Dummy image model with configurable delay and event probability."""

    EVENT_TYPES = {
        "car": "vehicle_detected",
        "fire": "fire_detected",
        "person_face": "person_detected",
        "ppe": "ppe_violation",
    }

    def __init__(
        self,
        model_id: str,
        inference_delay_ms: int = 30,
        detection_probability: float = 0.50,
    ):
        if model_id not in self.EVENT_TYPES:
            raise ValueError(f"Unsupported model: {model_id}")
        if inference_delay_ms < 0:
            raise ValueError("inference_delay_ms cannot be negative")

        override = os.getenv("SIMULATE_DETECTION_PROBABILITY")
        if override is not None:
            detection_probability = float(override)

        if not 0.0 <= detection_probability <= 1.0:
            raise ValueError("detection_probability must be between 0 and 1")

        self._model_id = model_id
        self._inference_delay_ms = int(inference_delay_ms)
        self._detection_probability = float(detection_probability)
        self._rng = random.Random()
        self._simulate_failure = (
            os.getenv("SIMULATE_AI_FAILURE", "0") == "1"
        )

        print(
            f"[MODEL INITIALIZED] model={model_id}, "
            f"delay_ms={self._inference_delay_ms}, "
            f"detection_probability={self._detection_probability:.2f}"
        )

    def predict(
        self,
        frame: CameraFrame,
        image: np.ndarray,
    ) -> InferenceResult:
        if image is None or not isinstance(image, np.ndarray):
            raise ValueError("Model received an invalid image")
        if image.ndim != 3 or image.shape[2] != 3:
            raise ValueError(f"Expected a three-channel BGR image; got {image.shape}")
        height, width = image.shape[:2]
        if height <= 0 or width <= 0:
            raise ValueError("Image dimensions are invalid")

        if self._inference_delay_ms:
            time.sleep(self._inference_delay_ms / 1000.0)

        if self._simulate_failure:
            raise RuntimeError("Simulated AI inference failure")

        detected = self._rng.random() < self._detection_probability

        result = InferenceResult(
            camera_id=frame.camera_id,
            frame_id=frame.frame_id,
            model_id=self._model_id,
            processed_at_ms=time.time_ns() // 1_000_000,
            status=(
                "simulated_detection"
                if detected
                else "simulated_no_detection"
            ),
            is_event=detected,
        )

        if detected:
            result.event_type = self.EVENT_TYPES[self._model_id]

        return result
