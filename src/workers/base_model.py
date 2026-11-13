from abc import ABC, abstractmethod

from src.proto.camera_frame_pb2 import CameraFrame
from src.proto.inference_result_pb2 import InferenceResult


class BaseAIModel(ABC):
    """Abstract interface for all AI models."""

    @abstractmethod
    def predict(self, frame: CameraFrame) -> InferenceResult:
        """Run inference on a camera frame."""
        pass

    @property
    def model_id(self) -> str:
        return self._model_id