import tempfile
import unittest
from pathlib import Path

import yaml

from src.deepstream.config import load_deepstream_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class DeepStreamConfigTests(unittest.TestCase):
    def test_default_configuration_loads(self):
        config = load_deepstream_config(
            PROJECT_ROOT / "config" / "deepstream.yaml"
        )

        self.assertEqual(config.source_mode, "files")
        self.assertEqual(config.max_frame_age_ms, 5000)
        self.assertFalse(config.publish_to_kafka)
        self.assertEqual(set(config.models), {"car", "fire", "person_face", "ppe"})
        self.assertTrue(all(not model.enabled for model in config.models.values()))
        self.assertTrue(all(model.workers == 1 for model in config.models.values()))

    def test_enabled_model_requires_engine_path(self):
        document = yaml.safe_load(
            (PROJECT_ROOT / "config" / "deepstream.yaml").read_text(
                encoding="utf-8"
            )
        )
        document["deepstream"]["models"]["person_face"]["enabled"] = True

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "deepstream.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "engine_path is required"):
                load_deepstream_config(path)

    def test_worker_count_must_be_positive(self):
        document = yaml.safe_load(
            (PROJECT_ROOT / "config" / "deepstream.yaml").read_text(
                encoding="utf-8"
            )
        )
        document["deepstream"]["models"]["car"]["workers"] = 0

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "deepstream.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "workers must be a positive integer"):
                load_deepstream_config(path)


if __name__ == "__main__":
    unittest.main()
