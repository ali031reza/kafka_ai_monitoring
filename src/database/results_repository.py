from psycopg import Cursor

from src.proto.inference_result_pb2 import InferenceResult


class ResultsRepository:
    """
    Handles PostgreSQL operations for inference results.

    Responsibilities:
        - Insert inference results
        - Link inference results to event images
        - Prevent duplicate inference records
        - Validate existing records
        - Return inference result IDs
    """

    def save(
        self,
        cursor: Cursor,
        result: InferenceResult,
        event_image_id: int | None = None,
    ) -> int:
        """
        Save inference metadata.

        Args:
            cursor:
                Active PostgreSQL transaction cursor.

            result:
                Deserialized inference result.

            event_image_id:
                Associated event image database ID.

        Returns:
            Existing or newly created inference result ID.
        """

        self._validate_result(
            result=result,
            event_image_id=event_image_id,
        )

        event_type = (
            result.event_type
            if result.is_event
            else None
        )

        query = """
            INSERT INTO inference_results (
                camera_id,
                frame_id,
                model_id,
                processed_at_ms,
                status,
                is_event,
                event_type,
                event_image_id
            )

            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

            ON CONFLICT (model_id, frame_id)
            DO NOTHING

            RETURNING id
        """

        params = (
            result.camera_id,
            result.frame_id,
            result.model_id,
            result.processed_at_ms,
            result.status,
            result.is_event,
            event_type,
            event_image_id,
        )

        cursor.execute(query, params)

        row = cursor.fetchone()

        if row is not None:
            return row[0]

        return self._get_existing_result_id(
            cursor=cursor,
            result=result,
            event_image_id=event_image_id,
        )

    def _get_existing_result_id(
        self,
        cursor: Cursor,
        result: InferenceResult,
        event_image_id: int | None,
    ) -> int:
        """Retrieve and validate an existing inference record."""

        query = """
            SELECT
                id,
                camera_id,
                status,
                is_event,
                event_type,
                event_image_id

            FROM inference_results

            WHERE model_id = %s
              AND frame_id = %s

            FOR UPDATE
        """

        cursor.execute(
            query,
            (
                result.model_id,
                result.frame_id,
            ),
        )

        row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "Existing inference result could not be retrieved"
            )

        existing_metadata = (
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
        )

        incoming_metadata = (
            result.camera_id,
            result.status,
            result.is_event,
            result.event_type if result.is_event else None,
            event_image_id,
        )

        if existing_metadata != incoming_metadata:
            raise ValueError(
                "Conflicting inference result for the same model/frame"
            )

        return row[0]

    @staticmethod
    def _validate_result(
        result: InferenceResult,
        event_image_id: int | None,
    ) -> None:
        """Validate inference result before database insertion."""

        if not result.camera_id or len(result.camera_id) > 50:
            raise ValueError("Invalid camera_id")

        if not result.frame_id or len(result.frame_id) > 100:
            raise ValueError("Invalid frame_id")

        if not result.model_id or len(result.model_id) > 50:
            raise ValueError("Invalid model_id")

        if not result.status or len(result.status) > 50:
            raise ValueError("Invalid result status")

        if result.processed_at_ms <= 0:
            raise ValueError("Invalid processing timestamp")

        if result.is_event:

            if not result.event_type:
                raise ValueError("Event type is missing")

            if len(result.event_type) > 100:
                raise ValueError("Event type is too long")

            if event_image_id is None:
                raise ValueError(
                    "Event result must reference a stored image"
                )

        elif event_image_id is not None:

            raise ValueError(
                "Non-event result cannot reference an event image"
            )