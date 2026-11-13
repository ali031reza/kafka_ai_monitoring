# Kafka AI Monitoring

A development-stage, event-driven video monitoring pipeline built around Kafka, PostgreSQL, Protobuf and Python. The project simulates 40 cameras, routes each camera's frames to its assigned model topics, runs mock AI inference, stores full JPEG images for simulated events, and persists inference metadata.

> **Current model stage:** all four model implementations are mock models. They consume decoded images and simulate inference latency and event probability. They do not perform real object detection yet.

## Architecture

```
Looping MP4 camera simulators
          |
          v
Database-driven camera/model router
          |
          v
Kafka frame topics (one topic per model)
          |
          v
Model-specific worker consumer groups
          |
          +--> Protobuf deserialization
          +--> JPEG decode (OpenCV)
          +--> Mock model inference
          +--> Full JPEG saved for positive events
          +--> Inference result published to Kafka
          |
          v
Kafka result topics
          |
          v
Results Processing Service
          |
          +--> Event image metadata
          +--> Inference result metadata
          |
          v
PostgreSQL

Processing failures --> Kafka dead-letter topic
```

## Model workers

Each model has its own package and class. The model-specific classes currently inherit the shared mock implementation in `src/workers/dummy_model.py`. This keeps the simulation behavior consistent while giving each model a clear replacement point for a future real inference implementation.

| Model ID | Python class | Simulated event type |
|---|---|---|
| `car` | `CarDetectionModel` | `vehicle_detected` |
| `fire` | `FireDetectionModel` | `fire_detected` |
| `person_face` | `PersonFaceDetectionModel` | `person_detected` |
| `ppe` | `PPEDetectionModel` | `ppe_violation` |

Model package locations:

- `src/workers/car/model.py`
- `src/workers/fire/model.py`
- `src/workers/person_face/model.py`
- `src/workers/ppe/model.py`

### Mock inference settings

The existing simulation settings are preserved:

- Inference delay: **30 ms per inference call**.
- Simulated positive-event probability: **0.50 (50%) per inference**.
- JPEG decoding occurs before inference in `AIWorker`.
- The original JPEG bytes are stored only when the mock model returns a positive event.
- The result includes the model-specific event type.

The 50% setting is a random simulation probability, not detection accuracy, precision, recall, or a claim that the sample video contains an object in half of its frames. Each inference draws an independent random outcome.

The settings are read from `config/config.yaml` for each model. The optional environment variable `SIMULATE_DETECTION_PROBABILITY` overrides the configured probability. Remove that variable to use the configured 0.50 value. `SIMULATE_AI_FAILURE=1` enables the existing failure-injection behavior.

## Load-test branch configuration

The `load-test` branch now carries the Phase H planning target of **8 FPS per camera** and **29 worker processes**. The 30 ms mock inference delay and 50% simulated event probability are unchanged. Input frame topics remain at 16 partitions each; results topics remain at 4 partitions each.

| Model | Historical camera assignments | Expected input at 8 FPS | Frame partitions | Configured worker processes |
|---|---:|---:|---:|---:|
| Car | 7 | 56 FPS | 16 | 4 |
| Fire | 12 | 96 FPS | 16 | 6 |
| Person/Face | 20 | 160 FPS | 16 | 10 |
| PPE | 17 | 136 FPS | 16 | 9 |
| **Total** | **56 assignments** | **448 FPS** | — | **29** |

The 7/12/20/17 camera counts are derived from the historical inference-result distribution used for initial planning. Verify the active rows in PostgreSQL `camera_model_assignments` before treating these rates as the authoritative offered load; recalculate the per-model plan if active assignments differ.

The worker plan assumes a nominal 20 FPS per worker and targets 80% utilization (16 input FPS per worker). These are planning assumptions, not measured capacity guarantees. Actual throughput must be measured on the target machine, including JPEG decoding, event-image writes, Kafka delivery and results persistence.

See [docs/LOAD_TESTING.md](docs/LOAD_TESTING.md) for the 10-minute Phase H benchmark procedure and local commands.

At 8 FPS per camera, 40 cameras produce 320 distinct source frames/second. With the historical 56 camera-to-model assignments, model-specific routing produces 448 frame records/second before drops or backpressure.

**Partition and worker counts are not being declared optimal here.** The 16 frame partitions and 4 result partitions per model are retained for this benchmark. Final sizing requires measured per-model throughput, per-partition lag, stale-frame skips, CPU, memory, JPEG decoding cost, event-image I/O and PostgreSQL result-ingestion capacity.

## Repository layout

```
config/
  config.yaml
  config_manager.py
infra/
  kafka/
    compose.yaml
  postgres/
    .env.example
    compose.yaml
    initdb/
    migrations/
schemas/
  camera_frame.proto
  inference_result.proto
  dead_letter.proto
src/
  database/
  ingestion/
  kafka/
  proto/
  results/
  storage/
  workers/
    ai_worker.py
    base_model.py
    dummy_model.py
    freshness_policy.py
    dead_letter_publisher.py
    car/
    fire/
    person_face/
    ppe/
  run_worker.py
  run_worker_pool.py
  run_results_processor.py
  run_multi_camera_simulator.py
  setup_topics.py
  inspect_dlt.py
```

The obsolete manual smoke scripts `src/test_consumer.py` and `src/test_producer.py` have been removed. The duplicate schema `schemas/schemasdead_letter.proto` has also been removed; `schemas/dead_letter.proto` is the canonical schema.

## Requirements

- Python 3.10 or newer.
- Docker Desktop with Docker Compose.
- A local MP4 sample at `Camera_sample/TEST.mp4` for the camera simulator. The video is local input and is not committed to this repository.
- Python packages used by the application: `confluent-kafka`, `psycopg[binary]`, `PyYAML`, `python-dotenv`, `protobuf`, `opencv-python`, `numpy` and `Pillow`.

Create and activate a virtual environment on Windows:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install confluent-kafka "psycopg[binary]" PyYAML python-dotenv protobuf opencv-python numpy Pillow
```

## Local setup

### 1. Configure PostgreSQL

Copy the example environment file once and replace its placeholder password:

```powershell
Copy-Item .\infra\postgres\.env.example .\infra\postgres\.env
```

Edit `infra/postgres/.env` and set a strong `POSTGRES_PASSWORD`. The file is ignored by Git.

Start PostgreSQL from the repository root:

```powershell
docker compose -f infra/postgres/compose.yaml up -d
```

### 2. Start Kafka

```powershell
docker compose -f infra/kafka/compose.yaml up -d
```

### 3. Create or reconcile Kafka topics

```powershell
python -m src.setup_topics
```

The topic manager creates missing topics and may increase existing partition counts to the values configured in `config/config.yaml`. Kafka cannot reduce a topic's partition count. Increasing partitions can change key-to-partition mapping, so review ordering implications before applying a partition change to an established production system.

## Run the application

Use separate PowerShell terminals from the repository root. Activate the virtual environment in each terminal.

**Terminal 1 — Results Processing Service**

```powershell
python -m src.run_results_processor
```

**Terminal 2 — Configured AI worker pool**

```powershell
python -m src.run_worker_pool
```

**Terminal 3 — 40 continuous simulated cameras at 8 FPS**

```powershell
python -m src.run_multi_camera_simulator --count 40 --fps 8
```

The simulator uses `Camera_sample/TEST.mp4` by default. It loops until Ctrl+C when `--max-frames` is omitted. Use `--video` to provide a different local video file. The source video's native FPS must be at least 8 FPS for the full target to be achieved.

To run one model worker manually instead of the full pool:

```powershell
python -m src.run_worker --model car
python -m src.run_worker --model fire
python -m src.run_worker --model person_face
python -m src.run_worker --model ppe
```

Do not start the manual worker for a model at the same time as the full pool unless intentionally adding another consumer to that model's group.

To inspect a dead-letter message:

```powershell
python -m src.inspect_dlt
```

Stop long-running services with `Ctrl+C`. Stop the camera simulator first, then the worker pool, then the results processor.

## Operational notes

- Camera-to-model routing is loaded from PostgreSQL; camera assignments are not hard-coded in the worker classes.
- Kafka frame records use the camera ID as the message key.
- Workers commit input offsets after result delivery, or after a failed record is successfully sent to the dead-letter topic.
- Event image files are stored under `data/event_images/`; this generated data is excluded from Git.
- PostgreSQL credentials belong in `infra/postgres/.env`; never commit real secrets.
- Avoid `docker compose down -v` unless you intentionally want to delete persistent database or Kafka volumes.
- The current worker pool and partition counts are development settings, not production capacity claims.

## Replacing mock models later

When real inference is introduced, replace the implementation behind each model package while preserving the worker contract:

```python
predict(frame: CameraFrame, image: numpy.ndarray) -> InferenceResult
```

The model must populate camera ID, frame ID, model ID, processing timestamp, status, event flag and event type consistently. Keep event-image metadata population in the worker/storage integration unless the architecture is deliberately revised.
