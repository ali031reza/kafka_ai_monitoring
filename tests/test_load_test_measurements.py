import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.load_test.resource_monitor import (
    _memory_to_mb,
    _parse_percent,
    _process_group_stats,
)
from src.load_test.summarize import (
    _read_text_auto,
    _summarize_lag,
)


class FakeProcess:
    def __init__(self, pid, created_at, user, system, rss):
        self.pid = pid
        self._created_at = created_at
        self._user = user
        self._system = system
        self._rss = rss

    def children(self, recursive=True):
        return []

    def create_time(self):
        return self._created_at

    def cpu_times(self):
        return SimpleNamespace(user=self._user, system=self._system)

    def memory_info(self):
        return SimpleNamespace(rss=self._rss)


class LoadTestMeasurementTests(unittest.TestCase):
    def test_process_tree_cpu_uses_time_deltas(self):
        state = {}
        first = FakeProcess(101, 10.0, 1.0, 0.0, 1024 * 1024)
        first_sample = _process_group_stats([first], state, 1.0)
        self.assertEqual(first_sample["cpu_percent"], 0.0)

        second = FakeProcess(101, 10.0, 1.5, 0.0, 2 * 1024 * 1024)
        second_sample = _process_group_stats([second], state, 2.0)
        self.assertAlmostEqual(second_sample["cpu_percent"], 50.0)
        self.assertEqual(second_sample["process_count"], 1)
        self.assertEqual(second_sample["rss_mb"], 2.0)

    def test_pid_reuse_resets_cpu_baseline(self):
        state = {}
        original = FakeProcess(101, 10.0, 1.0, 0.0, 1024)
        _process_group_stats([original], state, 1.0)

        reused = FakeProcess(101, 20.0, 20.0, 0.0, 2048)
        result = _process_group_stats([reused], state, 2.0)
        self.assertEqual(result["cpu_percent"], 0.0)

    def test_windows_utf16_log_is_decoded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "worker.log"
            expected = "[LOAD_TEST_WORKER_METRICS] {\"processed\": 12}"
            path.write_bytes(expected.encode("utf-16"))
            self.assertEqual(_read_text_auto(path), expected)

    def test_docker_memory_units(self):
        self.assertAlmostEqual(_memory_to_mb("100MiB"), 100.0)
        self.assertAlmostEqual(_memory_to_mb("1GiB"), 1024.0)
        self.assertAlmostEqual(_memory_to_mb("100MB"), 95.37)

    def test_percent_parser(self):
        self.assertAlmostEqual(_parse_percent("98.5%"), 98.5)
        self.assertIsNone(_parse_percent("n/a"))

    def test_lag_summary_tracks_first_last_and_unknown(self):
        rows = [
            {
                "group_id": "model_1_group",
                "stream_type": "frames",
                "model": "car",
                "timestamp_utc": "2026-11-14T00:00:00+00:00",
                "low_offset": "0",
                "committed_offset": "10",
                "lag": "5",
            },
            {
                "group_id": "model_1_group",
                "stream_type": "frames",
                "model": "car",
                "timestamp_utc": "2026-11-14T00:00:00+00:00",
                "low_offset": "0",
                "committed_offset": "-1001",
                "lag": "",
            },
            {
                "group_id": "model_1_group",
                "stream_type": "frames",
                "model": "car",
                "timestamp_utc": "2026-11-14T00:00:05+00:00",
                "low_offset": "0",
                "committed_offset": "20",
                "lag": "9",
            },
        ]

        result = _summarize_lag(rows)[
            ("model_1_group", "frames", "car")
        ]
        self.assertEqual(result["first_known_lag"], 5)
        self.assertEqual(result["last_known_lag"], 9)
        self.assertEqual(result["max_known_lag"], 9)
        self.assertEqual(result["lag_delta"], 4)
        self.assertEqual(result["max_unknown_partitions"], 1)


if __name__ == "__main__":
    unittest.main()
