"""Read enabled camera-to-model routes for the DeepStream source manager."""

from dataclasses import dataclass

from src.database.client import DatabaseClient


@dataclass(frozen=True)
class CameraModelRoute:
    camera_id: str
    rtsp_url: str | None
    credential_ref: str | None
    model_id: str
    result_topic: str


class DeepStreamSourceRepository:
    """Read source routing from the existing PostgreSQL schema.

    This repository returns credential references, never resolved secrets.
    It is intended for the later PostgreSQL integration phase; local MP4
    development sources are configured in config/deepstream.yaml.
    """

    def __init__(self, db_client: DatabaseClient) -> None:
        self._db = db_client

    def get_enabled_routes(self) -> list[CameraModelRoute]:
        query = """
            SELECT
                c.camera_id,
                c.rtsp_url,
                c.credential_ref,
                m.model_id,
                m.result_topic
            FROM camera_model_assignments AS a
            JOIN cameras AS c
              ON c.camera_id = a.camera_id
            JOIN models AS m
              ON m.model_id = a.model_id
            WHERE a.enabled = TRUE
              AND c.enabled = TRUE
              AND m.enabled = TRUE
            ORDER BY c.camera_id, m.model_id
        """
        rows = self._db.fetch_all(query)

        routes: list[CameraModelRoute] = []
        for camera_id, rtsp_url, credential_ref, model_id, result_topic in rows:
            if not camera_id or not model_id or not result_topic:
                raise ValueError("Database returned an incomplete camera/model route")
            routes.append(
                CameraModelRoute(
                    camera_id=camera_id,
                    rtsp_url=rtsp_url,
                    credential_ref=credential_ref,
                    model_id=model_id,
                    result_topic=result_topic,
                )
            )
        return routes
