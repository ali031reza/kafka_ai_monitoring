from pathlib import Path
import time

from config.config_manager import ConfigManager

from src.kafka.frame_consumer import KafkaFrameConsumer
from src.kafka.result_producer import KafkaResultProducer
from src.ingestion.frame_serializer import FrameSerializer
from src.workers.dummy_model import DummyAIModel


class ConsumerSmokeTest:

    def __init__(self):
        root = Path(__file__).resolve().parents[1]
        config = ConfigManager(root)

        kafka = config.get_section("kafka")
        fire = kafka["topics"]["fire"]
        servers = kafka["bootstrap_servers"]

        self._consumer = KafkaFrameConsumer(
            bootstrap_servers=servers,
            topic=fire["frames"],
            group_id=fire["group_id"],
            offset_reset=kafka["consumer"]["offset_reset"],
        )

        self._producer = KafkaResultProducer(servers)
        self._serializer = FrameSerializer()
        self._model = DummyAIModel("fire")
        self._result_topic = fire["results"]

    def run(self) -> None:
        print("Waiting for a Fire Detection frame...")

        deadline = time.monotonic() + 20

        try:
            while time.monotonic() < deadline:
                message = self._consumer.poll(timeout=1)

                if message is None:
                    continue

                frame = self._serializer.deserialize(
                    message.value()
                )

                result = self._model.predict(frame)

                self._producer.publish(
                    self._result_topic,
                    result,
                )

                # Only commit after the result reaches Kafka.
                self._consumer.commit(message)

                print("Result published:", self._result_topic)
                print("Input offset committed:", message.offset())
                return

            print("No frames received within 20 seconds.")

        finally:
            self._consumer.close()


if __name__ == "__main__":
    ConsumerSmokeTest().run()