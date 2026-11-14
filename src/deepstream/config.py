"""Typed loading and validation for config/deepstream.yaml."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


MODEL_IDS = ("car", "fire", "person_face", "ppe")
SOURCE_MODES = ("files", "postgres")


@dataclass(frozen=True)
class FileSourceConfig:
    camera_id: str
    uri: str


@dataclass(frozen=True)
class ModelConfig:
    model_id: str
    enabled: bool
    workers: int
    engine_path: str | None
    batch_size: int


@dataclass(frozen=True)
class DeepStreamConfig:
    runtime_api: str
    log_level: str
    source_mode: str
    max_frame_age_ms: int
    file_sources: tuple[FileSourceConfig, ...]
    models: dict[str, ModelConfig]
    publish_to_kafka: bool


def _mapping(value: Any, name: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a YAML mapping")
    return value


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def load_deepstream_config(path: Path | str | None = None) -> DeepStreamConfig:
    """Load and validate DeepStream settings.

    The default path is the repository-level config/deepstream.yaml.
    This loader validates application settings only; it does not import
    DeepStream or verify that model engine files exist.
    """
    config_path = (
        Path(path)
        if path is not None
        else Path(__file__).resolve().parents[2] / "config" / "deepstream.yaml"
    )

    with config_path.open("r", encoding="utf-8") as config_file:
        document = yaml.safe_load(config_file)

    root = _mapping(document, "configuration")
    deepstream = _mapping(root.get("deepstream"), "deepstream")
    runtime = _mapping(deepstream.get("runtime", {}), "deepstream.runtime")
    sources = _mapping(deepstream.get("sources", {}), "deepstream.sources")
    models_document = _mapping(deepstream.get("models"), "deepstream.models")
    results = _mapping(deepstream.get("results", {}), "deepstream.results")

    runtime_api = runtime.get("api", "pyservicemaker")
    log_level = runtime.get("log_level", "INFO")
    if not isinstance(runtime_api, str) or not runtime_api.strip():
        raise ValueError("deepstream.runtime.api must be a non-empty string")
    if not isinstance(log_level, str) or not log_level.strip():
        raise ValueError("deepstream.runtime.log_level must be a non-empty string")

    source_mode = sources.get("mode", "files")
    if source_mode not in SOURCE_MODES:
        raise ValueError(f"deepstream.sources.mode must be one of {SOURCE_MODES}")

    max_frame_age_ms = _positive_int(
        sources.get("max_frame_age_ms", 5000),
        "deepstream.sources.max_frame_age_ms",
    )

    files_document = sources.get("files", [])
    if not isinstance(files_document, list):
        raise ValueError("deepstream.sources.files must be a list")

    file_sources: list[FileSourceConfig] = []
    seen_camera_ids: set[str] = set()
    for index, source_document in enumerate(files_document):
        source = _mapping(source_document, f"deepstream.sources.files[{index}]")
        camera_id = source.get("camera_id")
        uri = source.get("uri")
        if not isinstance(camera_id, str) or not camera_id.strip():
            raise ValueError(f"File source {index} requires a non-empty camera_id")
        if not isinstance(uri, str) or not uri.strip():
            raise ValueError(f"File source {index} requires a non-empty uri")
        if camera_id in seen_camera_ids:
            raise ValueError(f"Duplicate file source camera_id: {camera_id}")
        seen_camera_ids.add(camera_id)
        file_sources.append(FileSourceConfig(camera_id=camera_id, uri=uri))

    unknown_models = set(models_document) - set(MODEL_IDS)
    if unknown_models:
        raise ValueError(f"Unsupported model IDs: {sorted(unknown_models)}")

    model_configs: dict[str, ModelConfig] = {}
    for model_id in MODEL_IDS:
        model_document = _mapping(
            models_document.get(model_id, {}),
            f"deepstream.models.{model_id}",
        )
        enabled = model_document.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError(f"deepstream.models.{model_id}.enabled must be boolean")

        workers = _positive_int(
            model_document.get("workers", 1),
            f"deepstream.models.{model_id}.workers",
        )
        batch_size = _positive_int(
            model_document.get("batch_size", 1),
            f"deepstream.models.{model_id}.batch_size",
        )

        engine_path = model_document.get("engine_path")
        if engine_path is not None and (
            not isinstance(engine_path, str) or not engine_path.strip()
        ):
            raise ValueError(
                f"deepstream.models.{model_id}.engine_path must be a non-empty string or null"
            )
        if enabled and engine_path is None:
            raise ValueError(
                f"deepstream.models.{model_id}.engine_path is required when the model is enabled"
            )

        model_configs[model_id] = ModelConfig(
            model_id=model_id,
            enabled=enabled,
            workers=workers,
            engine_path=engine_path,
            batch_size=batch_size,
        )

    publish_to_kafka = results.get("publish_to_kafka", False)
    if not isinstance(publish_to_kafka, bool):
        raise ValueError("deepstream.results.publish_to_kafka must be boolean")

    return DeepStreamConfig(
        runtime_api=runtime_api,
        log_level=log_level,
        source_mode=source_mode,
        max_frame_age_ms=max_frame_age_ms,
        file_sources=tuple(file_sources),
        models=model_configs,
        publish_to_kafka=publish_to_kafka,
    )
