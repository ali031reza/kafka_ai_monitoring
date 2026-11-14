import unittest

from src.deepstream.worker_manager import plan_model_workers


class WorkerManagerTests(unittest.TestCase):
    def test_round_robin_assigns_each_source_once(self):
        sources = ["cam-01", "cam-02", "cam-03", "cam-04", "cam-05", "cam-06"]
        plan = plan_model_workers("person_face", 3, sources)

        self.assertEqual(len(plan), 3)
        self.assertEqual(
            [assignment.worker_id for assignment in plan],
            [
                "person_face-worker-01",
                "person_face-worker-02",
                "person_face-worker-03",
            ],
        )
        assigned = [
            source_id
            for assignment in plan
            for source_id in assignment.source_ids
        ]
        self.assertCountEqual(assigned, sources)
        self.assertEqual(len(assigned), len(set(assigned)))

    def test_empty_source_list_creates_no_assignments(self):
        self.assertEqual(plan_model_workers("car", 2, []), ())

    def test_worker_count_cannot_exceed_source_count(self):
        with self.assertRaisesRegex(ValueError, "cannot exceed"):
            plan_model_workers("car", 3, ["cam-01", "cam-02"])

    def test_duplicate_source_ids_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be unique"):
            plan_model_workers("car", 1, ["cam-01", "cam-01"])


if __name__ == "__main__":
    unittest.main()
