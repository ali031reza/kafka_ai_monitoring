# Load Testing Guide

This guide is for the `load-test` branch. It does not change `main`.

## Test target

The branch configures 36 AI worker processes for the current 672 frame-records/second routing workload:

| Model | Input rate | Worker processes | Planning rate per worker |
|---|---:|---:|---:|
| car | 84 FPS | 5 | 20 FPS |
| fire | 144 FPS | 8 | 18 FPS |
| person_face | 240 FPS | 12 | 20 FPS |
| ppe | 204 FPS | 11 | 18.55 FPS |
| **Total** | **672 FPS** | **36** | — |

The target of 20 FPS/worker is a planning assumption, not a measured capacity guarantee. The person/face group has no spare aggregate capacity at 20 FPS/worker; the other groups have some nominal headroom.

Frame topics remain at 16 partitions each. Results topics remain at 4 partitions each. Worker replicas must not exceed the frame-topic partition count.

## What is measured

1. **AI worker throughput:** actual processed, skipped and failed frames per model; JPEG decode, inference, event-image save and result-publish average timings.
2. **Kafka lag:** partition-level lag for each model frame consumer group and the shared results processor consumer group.
3. **CPU and RAM:** system utilization plus the worker-pool and results-processor Python process trees.
4. **Results processor throughput:** committed result records/second, database transaction latency and Kafka offset-commit latency.
5. **PostgreSQL insertion throughput:** event-image metadata and inference-result metadata using the same repositories as the results processor.

## Prerequisites

- Run on the Windows machine that hosts the project, Kafka, PostgreSQL and camera simulator.
- Docker Desktop, Kafka and PostgreSQL must be running.
- The PostgreSQL schema and seeded camera/model routes must already exist.
- `Camera_sample/TEST.mp4` must exist. Its native FPS must be at least 12 FPS; the simulator caps output to the source video's FPS if the source is slower.
- Install the extra resource-monitor dependency:

```powershell
python -m pip install -r .\requirements-load-test.txt
```

## Prepare the test

Create the local output directory:

```powershell
New-Item -ItemType Directory -Force .\artifacts\load_test | Out-Null
```

Ensure the Kafka topics already exist. Do not run topic setup just for this test; the branch keeps the current 16 frame partitions and 4 result partitions per model.

Confirm that the configured worker counts are 5, 8, 12 and 11:

```powershell
python -c "import yaml; print(yaml.safe_load(open('config/config.yaml', encoding='utf-8'))['workers'])"
```

## Run the 5-minute live load test

Use separate PowerShell terminals from the repository root. Activate `.venv` in each terminal.

### Terminal 1 — Results processor

```powershell
python -m src.run_results_processor 2>&1 | Tee-Object -FilePath .\artifacts\load_test\results_processor.log
```

### Terminal 2 — AI worker pool

```powershell
python -m src.run_worker_pool 2>&1 | Tee-Object -FilePath .\artifacts\load_test\worker_pool.log
```

Wait until all 36 workers have started and joined their consumer groups before starting the camera simulator.

### Terminal 3 — Kafka consumer lag (5 minutes)

```powershell
python -m src.load_test.kafka_lag --interval 5 --duration 300
```

This samples all four model frame groups and the shared results processor group.

### Terminal 4 — CPU and RAM (5 minutes)

```powershell
python -m src.load_test.resource_monitor --interval 1 --duration 300
```

The monitor identifies the worker-pool and results-processor process trees by their Python module command lines. Start it after those services are running.

### Terminal 5 — 40 simulated cameras

```powershell
python -m src.run_multi_camera_simulator --count 40 --fps 12
```

Leave the simulator running for the full 5-minute measurement window. Stop it with Ctrl+C when the monitors finish. If the video source is below 12 FPS, the simulator will report and use the lower native rate; do not interpret that run as a 672 FPS offered-load test.

After the simulator stops, stop the worker pool and results processor with Ctrl+C. Their final metric lines are written to the log files.

## Test 5 — PostgreSQL metadata insertion benchmark

Run this after stopping the camera simulator, worker pool and results processor so the benchmark is not competing with the live pipeline.

Safe rollback mode (SQL statements execute, but transactions are rolled back; this does not measure durable commit throughput):

```powershell
python -m src.load_test.postgres_benchmark --count 2000
```

To measure durable transaction commits on the local development database, use the explicit `--commit` option:

```powershell
python -m src.load_test.postgres_benchmark --count 2000 --commit
```

Commit mode writes uniquely tagged benchmark rows to the real `event_images` and `inference_results` tables, measures the same per-result transaction pattern as the application, then deletes only rows tagged for that run. Run this only against your local development database. The script prints a run tag and reports cleanup success. If the process is forcibly killed before cleanup, use the run-tag cleanup SQL below after reviewing the tag.

```sql
DELETE FROM inference_results
WHERE left(camera_id, length('LT<run_tag>_')) = 'LT<run_tag>_';

DELETE FROM event_images
WHERE left(camera_id, length('LT<run_tag>_')) = 'LT<run_tag>_';
```

Replace `<run_tag>` with the exact tag printed by the benchmark. Delete inference results first because they reference event image rows.

## Generate the report

```powershell
python -m src.load_test.summarize
Get-Content .\artifacts\load_test\LOAD_TEST_REPORT.md
```

The report combines worker metrics, Kafka lag samples, resource samples, results processor metrics and the PostgreSQL benchmark JSON. Local output files are ignored by Git.

## How to judge the results

- Compare each model's measured aggregate processed FPS with its incoming FPS: car 84, fire 144, person/face 240 and PPE 204.
- A stable or declining Kafka lag is required. Rising lag means the group is not keeping up with offered load.
- Compare worker-pool CPU/RAM and whole-system CPU/RAM. Sustained CPU saturation or memory pressure can reduce throughput and destabilize the host.
- Results processor throughput must keep up with the combined result stream. If it does not, result-topic lag will rise even if AI workers keep processing.
- PostgreSQL commit-mode rows/second is a separate database-path benchmark. Compare it with the results processor's actual throughput; they are not identical because the live path also includes Kafka, Protobuf validation and image metadata handling.
- The simulator, camera video decoding/encoding, Kafka broker and Docker containers all consume the same machine resources. This is a single-host end-to-end test, not an isolated AI inference benchmark.

## Safety and branch scope

- All changes in this branch are instrumentation and load-test utilities plus the branch-specific worker-count configuration.
- The 30 ms mock inference delay and 50% event probability are unchanged.
- No Kafka partition counts, database schema, camera assignments or image-storage policy are changed.
- `main` remains unchanged. Do not merge this branch until the test results have been reviewed.

## Return to main

```powershell
git switch main
```

To resume the load test later:

```powershell
git switch load-test
git pull --ff-only origin load-test
```
