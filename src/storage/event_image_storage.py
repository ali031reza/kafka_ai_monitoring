import hashlib
import os
import re

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


@dataclass(frozen=True)
class StoredEventImage:
    """Metadata describing a successfully stored event image."""

    storage_key: str
    image_format: str
    file_size_bytes: int
    sha256: str
    captured_at_ms: int


class EventImageStorage:
    """
    Stores full JPEG frames associated with meaningful events.

    Responsibilities:
        - Validate incoming JPEG images
        - Generate deterministic storage paths
        - Prevent unsafe file paths
        - Write images atomically
        - Avoid duplicate image files
        - Return image metadata
    """

    def __init__(self, root_dir: Path):
        self._root_dir = Path(root_dir).resolve()
        self._root_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    @staticmethod
    def _safe_component(value: str) -> str:
        """Convert an identifier into a safe path component."""

        cleaned = re.sub(
            r"[^a-zA-Z0-9_-]",
            "_",
            value.strip(),
        )

        if not cleaned:
            raise ValueError("Invalid storage identifier")

        return cleaned[:128]

    def save_full_frame(
        self,
        camera_id: str,
        frame_id: str,
        captured_at_ms: int,
        image_format: str,
        image_data: bytes,
    ) -> StoredEventImage:

        # 1. Validate image format
        if image_format.lower() not in ("jpeg", "jpg"):
            raise ValueError("Only JPEG images are supported")

        # 2. Validate JPEG data
        if not image_data or not image_data.startswith(b"\xff\xd8"):
            raise ValueError("Invalid JPEG image")

        if captured_at_ms <= 0:
            raise ValueError("Invalid capture timestamp")

        # 3. Generate safe identifiers
        safe_camera = self._safe_component(camera_id)
        safe_frame = self._safe_component(frame_id)

        # 4. Organize images by UTC capture date
        captured_at = datetime.fromtimestamp(
            captured_at_ms / 1000,
            tz=timezone.utc,
        )

        relative_path = (
            Path(str(captured_at.year))
            / f"{captured_at.month:02d}"
            / f"{captured_at.day:02d}"
            / safe_camera
            / f"{safe_frame}.jpg"
        )

        destination = self._root_dir / relative_path

        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # 5. Calculate image checksum
        image_hash = hashlib.sha256(
            image_data
        ).hexdigest()

        # 6. Avoid duplicate writes
        if destination.exists():

            existing_hash = hashlib.sha256(
                destination.read_bytes()
            ).hexdigest()

            if existing_hash != image_hash:
                raise RuntimeError(
                    "Frame ID collision: existing image differs"
                )

        else:

            temporary_path = destination.with_name(
                f".{destination.name}.{uuid4().hex}.tmp"
            )

            try:
                with temporary_path.open("xb") as file:

                    file.write(image_data)
                    file.flush()

                    os.fsync(file.fileno())

                os.replace(
                    temporary_path,
                    destination,
                )

            finally:
                temporary_path.unlink(missing_ok=True)

        # 7. Return metadata
        return StoredEventImage(
            storage_key=relative_path.as_posix(),
            image_format="jpeg",
            file_size_bytes=len(image_data),
            sha256=image_hash,
            captured_at_ms=captured_at_ms,
        )