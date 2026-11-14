import unittest

from src.deepstream.result_adapter import build_inference_result
from src.storage.event_image_storage import StoredEventImage


class ResultAdapterTests(unittest.TestCase):
    def test_builds_non_event_result(self):
        result = build_inference_result(
            camera_id="camera-01",
            frame_id="frame-0001",
            model_id="person_face",
            captured_at_ms=1000,
            processed_at_ms=1100,
            status="processed",
        )

        self.assertEqual(result.camera_id, "camera-01")
        self.assertEqual(result.model_id, "person_face")
        self.assertFalse(result.is_event)
        self.assertEqual(result.captured_at_ms, 1000)

    def test_event_requires_stored_image(self):
        with self.assertRaisesRegex(ValueError, "stored event image"):
            build_inference_result(
                camera_id="camera-01",
                frame_id="frame-0001",
                model_id="person_face",
                captured_at_ms=1000,
                processed_at_ms=1100,
                status="event",
                event_type="person_detected",
            )

    def test_builds_event_result_from_stored_image_metadata(self):
        image = StoredEventImage(
            storage_key="2026/11/14/camera-01/frame-0001.jpg",
            image_format="jpeg",
            file_size_bytes=2048,
            sha256="a" * 64,
            captured_at_ms=1000,
        )
        result = build_inference_result(
            camera_id="camera-01",
            frame_id="frame-0001",
            model_id="person_face",
            captured_at_ms=1000,
            processed_at_ms=1100,
            status="event",
            event_type="person_detected",
            event_image=image,
        )

        self.assertTrue(result.is_event)
        self.assertEqual(result.event_type, "person_detected")
        self.assertEqual(result.storage_key, image.storage_key)
        self.assertEqual(result.image_sha256, image.sha256)

    def test_event_image_timestamp_must_match(self):
        image = StoredEventImage(
            storage_key="event.jpg",
            image_format="jpeg",
            file_size_bytes=100,
            sha256="b" * 64,
            captured_at_ms=999,
        )
        with self.assertRaisesRegex(ValueError, "timestamp does not match"):
            build_inference_result(
                camera_id="camera-01",
                frame_id="frame-0001",
                model_id="person_face",
                captured_at_ms=1000,
                processed_at_ms=1100,
                status="event",
                event_type="person_detected",
                event_image=image,
            )


if __name__ == "__main__":
    unittest.main()
