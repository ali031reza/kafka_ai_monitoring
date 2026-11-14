"""Project camera identity registry for DeepStream source IDs.

DeepStream source IDs are pipeline-local identifiers. This registry keeps
them separate from stable project camera IDs.
"""


class SourceRegistry:
    def __init__(self) -> None:
        self._camera_to_source: dict[str, int] = {}
        self._source_to_camera: dict[int, str] = {}

    def register(self, camera_id: str, source_id: int) -> None:
        if not isinstance(camera_id, str) or not camera_id.strip():
            raise ValueError("camera_id must be a non-empty string")
        if isinstance(source_id, bool) or not isinstance(source_id, int):
            raise ValueError("source_id must be an integer")
        if source_id < 0:
            raise ValueError("source_id cannot be negative")

        existing_source = self._camera_to_source.get(camera_id)
        existing_camera = self._source_to_camera.get(source_id)

        if existing_source == source_id and existing_camera == camera_id:
            return
        if existing_source is not None:
            raise ValueError(
                f"camera_id {camera_id!r} is already registered to source {existing_source}"
            )
        if existing_camera is not None:
            raise ValueError(
                f"source_id {source_id} is already registered to camera {existing_camera!r}"
            )

        self._camera_to_source[camera_id] = source_id
        self._source_to_camera[source_id] = camera_id

    def unregister(self, camera_id: str) -> int | None:
        source_id = self._camera_to_source.pop(camera_id, None)
        if source_id is not None:
            self._source_to_camera.pop(source_id, None)
        return source_id

    def camera_id_for_source(self, source_id: int) -> str:
        try:
            return self._source_to_camera[source_id]
        except KeyError as error:
            raise KeyError(f"Unknown DeepStream source_id: {source_id}") from error

    def source_id_for_camera(self, camera_id: str) -> int:
        try:
            return self._camera_to_source[camera_id]
        except KeyError as error:
            raise KeyError(f"Unknown camera_id: {camera_id}") from error

    def snapshot(self) -> dict[str, int]:
        """Return a copy of camera_id -> source_id mappings."""
        return dict(self._camera_to_source)
