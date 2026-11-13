from pathlib import Path

from config.config_manager import ConfigManager
from src.database.client import DatabaseClient
from src.database.camera_route_repository import CameraRouteRepository
from src.ingestion.frame_router import FrameRouter


class Application:

    def __init__(self):
        root = Path(__file__).resolve().parents[1]

        self._config = ConfigManager(root)
        db_config = self._config.get_database_config()

        print("Database host:", db_config["host"])
        print("Database port:", db_config["port"])
        print("Database name:", db_config["dbname"])
        print("Database user:", db_config["user"])

        self._database = DatabaseClient(
            **self._config.get_database_config()
        )

        repository = CameraRouteRepository(self._database)

        self._router = FrameRouter(repository)

    def run(self) -> None:
        self._router.refresh_routes()

        print("Configured cameras:", self._router.get_camera_count())

        for camera_id in ["C01", "C11", "C21"]:
            print(camera_id, "->", self._router.get_topics(camera_id))


if __name__ == "__main__":
    Application().run()