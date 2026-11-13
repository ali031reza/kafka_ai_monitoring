from abc import ABC, abstractmethod

import numpy as np

from src.proto.camera_frame_pb2 import CameraFrame
from src.proto.inference_result_pb2 import InferenceResult


class BaseAIModel(ABC):
    """Common interface for image-based AI models."""

    @abstractmethod
    def predict(
        self,
        frame: CameraFrame,
        image: np.ndarray,
    ) -> InferenceResult:
        """Run inference using frame metadata and a decoded BGR image."""
        raise NotImplementedError

    @property
    def model_id(self) -> str:
        return self._model_id
