"""DeepStream branch configuration entry point.

The actual NVIDIA pipeline is intentionally not started here yet. First
validate the configuration and runtime on the target laptop, then implement
the release-specific pipeline factory in the next development step.
"""

import argparse
import json
from pathlib import Path

from src.deepstream.config import load_deepstream_config


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Kafka AI Monitoring DeepStream configuration checker"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to deepstream.yaml (defaults to config/deepstream.yaml)",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate and print the DeepStream configuration",
    )
    args = parser.parse_args()

    if not args.check_config:
        parser.error(
            "Pipeline execution is not implemented yet; use --check-config "
            "until the DeepStream runtime and model are validated."
        )

    config = load_deepstream_config(args.config)
    summary = {
        "runtime_api": config.runtime_api,
        "log_level": config.log_level,
        "source_mode": config.source_mode,
        "max_frame_age_ms": config.max_frame_age_ms,
        "file_sources": [
            {"camera_id": source.camera_id, "uri": source.uri}
            for source in config.file_sources
        ],
        "models": {
            model_id: {
                "enabled": model.enabled,
                "workers": model.workers,
                "engine_path": model.engine_path,
                "batch_size": model.batch_size,
            }
            for model_id, model in config.models.items()
        },
        "publish_to_kafka": config.publish_to_kafka,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if not any(model.enabled for model in config.models.values()):
        print("No models are enabled; this is the initial configuration scaffold.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
