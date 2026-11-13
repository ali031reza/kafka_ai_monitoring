"""PPE detection model adapter for the current mock-inference stage."""
from src.workers.dummy_model import DummyAIModel


class PPEDetectionModel(DummyAIModel):
    """Simulates PPE violation detection using the shared mock inference behavior."""

    def __init__(
        self,
        inference_delay_ms: int = 30,
        detection_probability: float = 0.50,
    ) -> None:
        super().__init__(
            model_id="ppe",
            inference_delay_ms=inference_delay_ms,
            detection_probability=detection_probability,
        )
