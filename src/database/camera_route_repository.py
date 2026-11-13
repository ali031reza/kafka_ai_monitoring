from collections import defaultdict
from .client import DatabaseClient


class CameraRouteRepository:
    """Retrieves camera-to-model routing configuration."""

    def __init__(self, db_client: DatabaseClient):
        self._db = db_client

    def get_all_routes(self) -> dict[str, list[str]]:
        query = """
            SELECT a.camera_id, m.input_topic
            FROM camera_model_assignments a
            JOIN cameras c ON a.camera_id = c.camera_id
            JOIN models m ON a.model_id = m.model_id
            WHERE a.enabled = TRUE
              AND c.enabled = TRUE
              AND m.enabled = TRUE
            ORDER BY a.camera_id, m.model_id
        """

        rows = self._db.fetch_all(query)

        routes = defaultdict(list)

        for camera_id, topic in rows:
            routes[camera_id].append(topic)

        return dict(routes)