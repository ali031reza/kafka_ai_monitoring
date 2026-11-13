import re

from pathlib import PurePosixPath

from psycopg import Cursor

from src.proto.inference_result_pb2 import InferenceResult


class ImageRepository:
    """
    Handles PostgreSQL operations for event image metadata.

    Responsibilities:
        - Insert event image metadata
        - Retrieve existing image records
        - Prevent duplicate image records
        - Validate image metadata consistency
        - Return database image IDs
    """

    def save(
        self,
        cursor: Cursor,
        result: InferenceResult,
    ) -> int:
        """
        Save event image metadata.

        The caller must provide an active database cursor.

        Returns:
            Existing or newly created event image ID.
        """

        self._validate_result(result)

        query = """
            INSERT INTO event_images (
                camera_id,
                frame_id,
                storage_key,
                image_format,
                file_size_bytes,
                sha256,
                captured_at_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)

            ON CONFLICT (camera_id, frame_id)
            DO NOTHING

            RETURNING id
        """

        params = (
            result.camera_id,
            result.frame_id,
            result.storage_key,
            result.image_format,
            result.image_size_bytes,
            result.image_sha256,
            result.captured_at_ms,
        )

        cursor.execute(query, params)

        row = cursor.fetchone()

        if row is not None:
            return row[0]

        # An image for this camera/frame already exists.
        return self._get_existing_image_id(
            cursor=cursor,
            result=result,
        )

    def _get_existing_image_id(
        self,
        cursor: Cursor,
        result: InferenceResult,
    ) -> int:
        """
        Retrieve an existing image and verify its metadata.

        A duplicate frame must reference identical image data.
        """

        query = """
            SELECT
                id,
                storage_key,
                image_format,
                file_size_bytes,
                sha256,
                captured_at_ms

            FROM event_images

            WHERE camera_id = %s
              AND frame_id = %s

            FOR UPDATE
        """

        cursor.execute(
            query,
            (
                result.camera_id,
                result.frame_id,
            ),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "Existing event image could not be retrieved"
            )

        image_id = row[0]

        existing_metadata = (
            row[1],
            row[2],
            row[3],
            row[4].strip(),
            row[5],
        )

        incoming_metadata = (
            result.storage_key,
            result.image_format,
            result.image_size_bytes,
            result.image_sha256,
            result.captured_at_ms,
        )

        if existing_metadata != incoming_metadata:
            raise ValueError(
                "Conflicting image metadata for the same camera/frame"
            )

        return image_id

    @staticmethod
    def _validate_result(result: InferenceResult) -> None:
        """Validate image metadata before database insertion."""

        if not result.is_event:
            raise ValueError(
                "Cannot save image metadata for a non-event result"
            )

        if not result.camera_id or len(result.camera_id) > 50:
            raise ValueError("Invalid camera_id")

        if not result.frame_id or len(result.frame_id) > 100:
            raise ValueError("Invalid frame_id")

        if not result.storage_key:
            raise ValueError("Missing image storage key")

        if "\\" in result.storage_key:
            raise ValueError("Invalid image storage key")

        if result.storage_key.startswith("/"):
            raise ValueError("Image storage key must be relative")

        if any(
            part in ("", ".", "..")
            for part in result.storage_key.split("/")
        ):
            raise ValueError("Unsafe image storage key")

        path = PurePosixPath(result.storage_key)

        if path.is_absolute():
            raise ValueError("Absolute image paths are not allowed")

        if result.image_format.lower() not in ("jpeg", "jpg"):
            raise ValueError("Unsupported image format")

        if result.image_size_bytes <= 0:
            raise ValueError("Invalid image size")

        if not re.fullmatch(
            r"[a-fA-F0-9]{64}",
            result.image_sha256,
        ):
            raise ValueError("Invalid SHA-256 checksum")

        if result.captured_at_ms <= 0:
            raise ValueError("Invalid capture timestamp")