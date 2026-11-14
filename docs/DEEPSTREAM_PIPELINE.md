# DeepStream Pipeline branch

## Branch purpose

The DeepStream Pipeline branch is an isolated implementation path for
replacing Kafka frame transport and Python mock-worker inference with a
DeepStream video pipeline. Kafka remains in place for inference results and
the existing results-processing backend.

The branch was created from load-test. That branch is 32 commits ahead of
main and already contains the main implementation plus load-testing
changes, so no separate copy from main was necessary.

## Existing project code retained

The current Kafka architecture remains intact. The DeepStream package
reuses existing project components rather than maintaining duplicate
implementations:

| Existing component | Planned reuse |
|---|---|
| src/database/client.py | PostgreSQL connections and transactions |
| src/kafka/result_producer.py | Kafka result publishing with delivery confirmation |
| src/storage/event_image_storage.py | Event-only JPEG storage and metadata |
| src/proto/inference_result_pb2.py | Existing inference result Protobuf |
| src/results/results_processing_service.py | Existing result persistence service |
| config/config_manager.py | Existing application configuration and database credentials |

The existing model classes are currently mock implementations. No real
model weights or TensorRT engines are included in the tracked repository
tree.

## New DeepStream components

| Path | Responsibility | Current status |
|---|---|---|
| config/deepstream.yaml | Independent DeepStream source/model/worker settings | Added |
| src/deepstream/config.py | Typed configuration loading and validation | Added |
| src/deepstream/worker_manager.py | Deterministic source-to-worker planning | Added |
| src/deepstream/source_manager.py | Stable camera ID to DeepStream source ID registry | Added |
| src/deepstream/result_adapter.py | Project-level outcome to existing Protobuf adapter | Added |
| src/deepstream/result_publisher.py | Routes results through the existing Kafka producer | Added |
| src/deepstream/runtime_check.py | Checks GStreamer and core NVIDIA plugins | Added |
| src/deepstream/app.py | Configuration validation entry point | Added |
| src/database/deepstream_source_repository.py | Reads enabled camera/model routes from PostgreSQL | Added; not wired into a running pipeline |
| infra/deepstream/ | Runtime packaging notes | Added |

The actual pipeline factory, DeepStream metadata extraction, TensorRT
engine configuration, and dynamic source lifecycle are intentionally
deferred until the laptop runtime and first model are validated. This
avoids committing code against an unconfirmed DeepStream API/version.

## Worker scaling contract

Worker counts are configured independently for each model in
config/deepstream.yaml. The initial values are placeholders and all models
are disabled until model engines are available.

The first scale-out experiment will use process-level replicas of one
model, tested at 1, 2, 3, and 4 workers where hardware permits. The worker
planner assigns distinct sources to replicas using round-robin assignment.
It does not split frames from one camera across multiple workers.

Adding workers is not assumed to increase throughput linearly. GPU memory,
compute utilization, decoder capacity, latency, and thermal behavior must
be measured.

## Commands

From the repository root:

Validate the DeepStream configuration:

    python -m src.deepstream.app --check-config

Check GStreamer and required NVIDIA plugins from inside the selected
DeepStream runtime:

    python -m src.deepstream.runtime_check

Run the package unit tests:

    python -m unittest discover -s tests/deepstream -v

The runtime diagnostic is expected to fail on a normal host that does not
have the DeepStream/GStreamer NVIDIA plugins installed. Run it inside the
NVIDIA environment after Phase 0 setup.

## Current limitations

- No DeepStream pipeline is started yet.
- No real project model engine is included.
- The initial config contains no video sources and has all models disabled.
- PostgreSQL route reading is implemented as a repository but is not yet
  connected to source lifecycle management.
- Result publishing is an adapter around the existing producer; end-to-end
  Kafka integration has not yet been exercised.
- The existing InferenceResult schema does not contain detection boxes,
  class IDs, labels, or confidence values.
- No Dockerfile or Compose service is pinned until GPU/runtime compatibility
  has been established.

## Next implementation steps

1. Complete the hardware/runtime checks in Phase 0.
2. Add a release-compatible pipeline factory and one local MP4 source.
3. Run one real model and validate its metadata.
4. Use the worker planner to test 1/2/3/4 replicas of that same model.
5. Extend to multiple sources and then integrate the remaining models.
