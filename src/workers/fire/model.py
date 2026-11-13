"""Fire detection model adapter for the current mock-inference stage."""
from src.workers.dummy_model import DummyAIModel


class FireDetectionModel(DummyAIModel):
    """Simulates fire detection using the shared mock inference behavior."""

    def __init__(
        self,
        inference_delay_ms: int = 30,
        detection_probability: float = 0.50,
    ) -> None:
        super().__init__(
            model_id="fire",
            inference_delay_ms=inference_delay_ms,
            detection_probability=detection_probability,
        )
