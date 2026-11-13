from src.proto.camera_frame_pb2 import CameraFrame


class FrameSerializer:
    """Serializes camera frames using Protobuf."""

    def serialize(
        self,
        camera_id: str,
        frame_id: str,
        timestamp_ms: int,
        sequence_number: int,
        image_data: bytes,
    ) -> bytes:

        message = CameraFrame(
            camera_id=camera_id,
            frame_id=frame_id,
            timestamp_ms=timestamp_ms,
            sequence_number=sequence_number,
            image_data=image_data,
            image_format="jpeg",
        )

        return message.SerializeToString()

    def deserialize(self, data: bytes) -> CameraFrame:
        message = CameraFrame()
        message.ParseFromString(data)
        return message