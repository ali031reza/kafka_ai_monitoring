import os
from pathlib import Path

import yaml
from dotenv import load_dotenv


class ConfigManager:
    """Loads application settings and environment variables."""

    def __init__(self, project_root: Path):
        self._project_root = project_root

        load_dotenv(
            project_root / "infra" / "postgres" / ".env"
        )

        config_path = project_root / "config" / "config.yaml"

        with config_path.open("r", encoding="utf-8") as file:
            self._config = yaml.safe_load(file)

        if not isinstance(self._config, dict):
            raise ValueError("Invalid application configuration")

    def get_section(self, section: str) -> dict:
        if section not in self._config:
            raise KeyError(f"Missing configuration section: {section}")

        return self._config[section]

    def get_database_config(self) -> dict:
        settings = self.get_section("database")

        return {
            "host": settings["host"],
            "port": settings["port"],
            "dbname": settings["name"],
            "user": settings["user"],
            "password": os.environ["POSTGRES_PASSWORD"],
        }
        