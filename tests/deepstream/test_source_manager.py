import unittest

from src.deepstream.source_manager import SourceRegistry


class SourceRegistryTests(unittest.TestCase):
    def test_register_and_lookup(self):
        registry = SourceRegistry()
        registry.register("camera-01", 0)

        self.assertEqual(registry.camera_id_for_source(0), "camera-01")
        self.assertEqual(registry.source_id_for_camera("camera-01"), 0)
        self.assertEqual(registry.snapshot(), {"camera-01": 0})

    def test_register_is_idempotent_for_same_pair(self):
        registry = SourceRegistry()
        registry.register("camera-01", 0)
        registry.register("camera-01", 0)
        self.assertEqual(registry.snapshot(), {"camera-01": 0})

    def test_source_id_cannot_be_reused(self):
        registry = SourceRegistry()
        registry.register("camera-01", 0)
        with self.assertRaisesRegex(ValueError, "already registered"):
            registry.register("camera-02", 0)

    def test_unregister_releases_mapping(self):
        registry = SourceRegistry()
        registry.register("camera-01", 0)

        self.assertEqual(registry.unregister("camera-01"), 0)
        self.assertEqual(registry.snapshot(), {})


if __name__ == "__main__":
    unittest.main()
