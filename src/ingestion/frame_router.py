from src.database.camera_route_repository import CameraRouteRepository


class FrameRouter:
    """Manages cached camera-to-Kafka-topic routing."""

    def __init__(self, repository: CameraRouteRepository):
        self._repository = repository
        self._routes: dict[str, tuple[str, ...]] = {}

    def refresh_routes(self) -> int:
        """Load the latest routing configuration from PostgreSQL."""

        routes = self._repository.get_all_routes()

        # Replace the cache only after successfully loading all routes.
        self._routes = {
            camera_id: tuple(topics)
            for camera_id, topics in routes.items()
        }

        return len(self._routes)

    def get_topics(self, camera_id: str) -> tuple[str, ...]:
        """Return topics assigned to a camera."""

        return self._routes.get(camera_id, ())

    def get_camera_count(self) -> int:
        """Return the number of configured cameras."""

        return len(self._routes)