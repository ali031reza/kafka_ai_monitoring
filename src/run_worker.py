import argparse
from pathlib import Path

from config.config_manager import ConfigManager

from src.kafka.frame_consumer import KafkaFrameConsumer
from src.kafka.result_producer import KafkaResultProducer
from src.ingestion.frame_serializer import FrameSerializer

from src.workers.ai_worker import AIWorker
from src.workers.base_model import BaseAIModel
from src.workers.car import CarDetectionModel
from src.workers.fire import FireDetectionModel
from src.workers.person_face import PersonFaceDetectionModel
from src.workers.ppe import PPEDetectionModel
from src.workers.freshness_policy import FrameFreshnessPolicy
from src.workers.dead_letter_publisher import DeadLetterPublisher

from src.storage.event_image_storage import EventImageStorage


MODEL_CLASSES: dict[str, type[BaseAIModel]] = {
    "car": CarDetectionModel,
    "fire": FireDetectionModel,
    "person_face": PersonFaceDetectionModel,
    "ppe": PPEDetectionModel,
}


class WorkerApplication:
    """Initializes one independent worker instance for a selected model."""

    def __init__(
        self,
        model_name: str,
        worker_instance_id: str | None = None,
    ):
        if model_name not in MODEL_CLASSES:
            raise ValueError(
                f"Unsupported model '{model_name}'. "
                f"Choose from: {', '.join(MODEL_CLASSES)}"
            )

        self._model_name = model_name
        root = Path(__file__).resolve().parents[1]
        config = ConfigManager(root)

        kafka = config.get_section("kafka")
        model_config = kafka["topics"][model_name]
        storage_config = config.get_section("storage")["event_images"]

        group_id = model_config["group_id"]
        client_id = (
            f"{group_id}-{worker_instance_id}"
            if worker_instance_id
            else f"{group_id}-manual"
        )

        consumer = KafkaFrameConsumer(
            bootstrap_servers=kafka["bootstrap_servers"],
            topic=model_config["frames"],
            group_id=group_id,
            offset_reset=kafka["consumer"]["offset_reset"],
            client_id=client_id,
        )

        producer = KafkaResultProducer(kafka["bootstrap_servers"])
        self._producer = producer

        dead_letter = DeadLetterPublisher(
            producer=producer,
            topic=kafka["dead_letter"]["topic"],
            model_id=model_name,
        )

        if not storage_config["enabled"]:
            raise RuntimeError("Event image storage is disabled")

        image_storage = EventImageStorage(
            root_dir=root / storage_config["root_dir"]
        )

        model_class = MODEL_CLASSES[model_name]
        model = model_class(
            inference_delay_ms=model_config.get(
                "inference_delay_ms", 30
            ),
            detection_probability=model_config.get(
                "detection_probability", 0.50
            ),
        )

        self._worker = AIWorker(
            consumer=consumer,
            producer=producer,
            serializer=FrameSerializer(),
            model=model,
            model_id=model_name,
            result_topic=model_config["results"],
            freshness_policy=FrameFreshnessPolicy(
                max_age_ms=model_config["max_frame_age_ms"]
            ),
            dead_letter_publisher=dead_letter,
            image_storage=image_storage,
            max_retries=kafka["retry"]["max_retries"],
            retry_delay_ms=kafka["retry"]["delay_ms"],
        )

        print(
            f"[WORKER CONFIGURED] model={model_name}, "
            f"model_class={model_class.__name__}, "
            f"client_id={client_id}, group_id={group_id}"
        )

    def run(self, once: bool = False, stop_event=None) -> None:
        try:
            self._worker.run(
                max_messages=1 if once else None,
                stop_event=stop_event,
            )
        finally:
            self._producer.close()


class WorkerCLI:
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description="Kafka AI Monitoring Worker"
        )
        self._parser.add_argument(
            "--model",
            required=True,
            choices=list(MODEL_CLASSES),
        )
        self._parser.add_argument(
            "--once",
            action="store_true",
            help="Process one message and exit",
        )
        self._parser.add_argument(
            "--worker-id",
            default="manual",
            help="Unique client ID suffix; consumer group stays shared",
        )

    def run(self) -> None:
        args = self._parser.parse_args()
        try:
            app = WorkerApplication(
                model_name=args.model,
                worker_instance_id=args.worker_id,
            )
            app.run(once=args.once)
        except KeyboardInterrupt:
            print("\nAI worker stopped by user.")


if __name__ == "__main__":
    WorkerCLI().run()
