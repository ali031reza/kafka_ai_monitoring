from pathlib import Path

from config.config_manager import ConfigManager
from src.kafka.topic_manager import KafkaTopicManager


class TopicSetupApplication:

    def __init__(self):
        root = Path(__file__).resolve().parents[1]

        self._config = ConfigManager(root)

        self._topic_manager = KafkaTopicManager(
            self._config.get_section("kafka")
        )

    def run(self) -> None:
        self._topic_manager.ensure_topics()


if __name__ == "__main__":
    TopicSetupApplication().run()