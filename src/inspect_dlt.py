import time
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException

from config.config_manager import ConfigManager
from src.proto.dead_letter_pb2 import DeadLetterEvent


class DLTInspector:
    """Reads and displays Kafka dead-letter events."""

    def __init__(self):
        root = Path(__file__).resolve().parents[1]

        config = ConfigManager(root)
        kafka = config.get_section("kafka")

        self._topic = kafka["dead_letter"]["topic"]

        self._consumer = Consumer({
            "bootstrap.servers": ",".join(
                kafka["bootstrap_servers"]
            ),
            "group.id": f"dlt-inspector-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        self._consumer.subscribe([self._topic])

    def run(self, timeout_seconds: int = 15) -> None:
        deadline = time.monotonic() + timeout_seconds

        try:
            while time.monotonic() < deadline:
                message = self._consumer.poll(1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                event = DeadLetterEvent()
                event.ParseFromString(message.value())

                print("Camera:", event.camera_id)
                print("Model:", event.model_id)
                print("Frame:", event.frame_id)
                print("Error:", event.error_type)
                print("Details:", event.error_message)
                print("Attempts:", event.attempts)
                print("Source topic:", event.source_topic)
                print("Source offset:", event.source_offset)

                return

            print("No DLT message found.")

        finally:
            self._consumer.close()


if __name__ == "__main__":
    DLTInspector().run()