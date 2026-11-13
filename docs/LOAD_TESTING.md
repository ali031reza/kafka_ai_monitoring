# Phase H — 8 FPS / 40-Camera Sustained Load Test

This guide applies to the `load-test` branch only. It does not change or authorize merging into `main`.

## 1. Agreed Phase H target

| Parameter | Target |
|---|---:|
| Cameras | 40 |
| FPS per camera | 8 |
| Unique source frames | 320 frames/s |
| Resolution | 1920 × 1080 |
| Encoding | JPEG |
| Inference delay | 30 ms |
| Simulated event probability | 0.50 |
| Nominal worker planning capacity | 20 FPS/worker |
| Planned worker utilization | 80% (16 input FPS/worker) |
| Total planned workers | 29 |

The 29-worker distribution in `config/config.yaml` is:

| Model | Historical camera assignments | Expected input at 8 FPS | Planned workers | Nominal capacity at 20 FPS/worker | Planned utilization |
|---|---:|---:|---:|---:|---:|
| car | 7 | 56 FPS | 4 | 80 FPS | 70.0% |
| fire | 12 | 96 FPS | 6 | 120 FPS | 80.0% |
| person_face | 20 | 160 FPS | 10 | 200 FPS | 80.0% |
| ppe | 17 | 136 FPS | 9 | 180 FPS | 75.6% |
| **Total** | **56 assignments** | **448 FPS** | **29** | **580 FPS** | **77.2%** |

**Important routing qualification:** The 7/12/20/17 camera counts come from the historical inference-result distribution observed during project testing. They are a sizing assumption, not proof of the current enabled rows in `camera_model_assignments`. Inspect that table and verify the active mapping before treating 56/96/160/136 FPS as the authoritative offered load. Recalculate the per-model worker plan if the active assignment counts differ.

The 320 source frames/s become 448 model-topic records/s only if the active routing inventory has 56 assignments and every camera supplies 8 FPS to each assigned model.

## 2. Partition and worker policy

- Input frame topics remain at **16 partitions per model**.
- Results topics remain at **4 partitions per model**.
- Worker replicas are configured independently from partitions.
- The 29 planned workers are below the 16-partition limit for each model.
- Do not run `src.setup_topics` as part of this benchmark; this test does not change partition counts.
- Keep `camera_id` as the Kafka record key for this test.

Because records are keyed by `camera_id`, one camera's records for a topic normally stay on one partition. Several camera keys may hash to the same partition, and some partitions may receive no records. Therefore, partition count alone does not guarantee even load distribution.

A partition with no committed offset is not automatically proof of a backlog. Compare its low and high watermarks: an empty partition (high offset equals low offset) may legitimately have no commit. A partition with data (high offset greater than low offset) but no valid committed offset is unresolved and must not be counted as zero lag.

## 3. Prerequisites and safety

- Run on the Windows host that runs the project, Kafka, PostgreSQL and the simulator.
- Docker Desktop, Kafka and PostgreSQL must be healthy.
- The seeded database routing must include the intended 40 active cameras.
- `Camera_sample/TEST.mp4` must exist and have a native frame rate of at least 8 FPS. The simulator cannot produce a true 8 FPS output if its source video is slower.
- Activate the project virtual environment in every PowerShell terminal.
- Install the load-test dependency if it is not already installed:

    python -m pip install -r .\requirements-load-test.txt

- Create the local output directory:

    New-Item -ItemType Directory -Force .\artifacts\load_test | Out-Null

- Do not run a second worker pool, camera simulator or results processor from a different Python installation. Use only the repository's `.venv`.
- Do not delete Kafka topics, reset consumer offsets, clear database tables or run `docker compose down -v` for this test.

Before starting, verify the branch and review local changes:

    git branch --show-current
    git status --short

The branch must be `load-test`. If there are local changes, review and preserve them before pulling or switching branches.

Verify the configured targets:

    python -c "import yaml; c=yaml.safe_load(open('config/config.yaml', encoding='utf-8')); print('cameras:', c['cameras']); print('workers:', c['workers']); print('partitions:', {k:v['partitions'] for k,v in c['kafka']['topics'].items()})"

Expected worker counts: `{'car': 4, 'fire': 6, 'person_face': 10, 'ppe': 9}`. Expected camera FPS: `8`. Expected frame partitions: `16` per model.

Inspect the actual database assignment table before the test:

    docker exec postgres-postgres-1 psql -U ai_app -d ai_monitoring -c "\\d+ public.camera_model_assignments"
    docker exec postgres-postgres-1 psql -U ai_app -d ai_monitoring -c "SELECT * FROM public.camera_model_assignments ORDER BY 1, 2;"

Use the table's enabled/active fields, if present, when counting assignments. Do not use historical inference rows as a substitute for the active routing table.

## 4. Ten-minute end-to-end test

Use separate PowerShell terminals, all from the repository root and all using the same `.venv`.

### Terminal 1 — Results processor

    python -m src.run_results_processor 2>&1 | Tee-Object -FilePath .\artifacts\load_test\results_processor.log

Start exactly one instance.

### Terminal 2 — AI worker pool

    python -m src.run_worker_pool 2>&1 | Tee-Object -FilePath .\artifacts\load_test\worker_pool.log

The pool reads the replica counts from `config/config.yaml`. Confirm that it reports 4 car, 6 fire, 10 person_face and 9 ppe processes (29 total). Wait until the workers have started and joined their consumer groups before starting the simulator.

### Terminal 3 — Forty continuous camera simulators

    python -m src.run_multi_camera_simulator --count 40 --fps 8 2>&1 | Tee-Object -FilePath .\artifacts\load_test\camera_simulator.log

Do not pass `--max-frames`: without that limit, the simulator loops the MP4 until Ctrl+C. Confirm that 40 camera threads start and that the simulator reports an 8 FPS target per camera (320 unique source frames/s). If the source video caps the output below 8 FPS, stop and do not classify that run as the 8 FPS benchmark.

### Terminal 4 — Kafka lag sampling (10 minutes)

Start after all 40 camera threads are running:

    python -m src.load_test.kafka_lag --interval 5 --duration 600 --output .\artifacts\load_test\kafka_lag.csv

This samples all four frame consumer groups and the shared results processor group.

### Terminal 5 — CPU and RAM sampling (10 minutes)

Start at approximately the same time as Terminal 4:

    python -m src.load_test.resource_monitor --interval 1 --duration 600 --output .\artifacts\load_test\resources.csv

The monitor measures whole-system utilization and the worker-pool/results-processor process trees.

### End the live run

1. Let the lag and resource monitors finish their 600-second window.
2. Stop the camera simulator with Ctrl+C.
3. Keep workers and the results processor running briefly so they can finish in-flight work and drain any short backlog.
4. Run a final 30-second lag sample:

       python -m src.load_test.kafka_lag --interval 2 --duration 30 --output .\artifacts\load_test\kafka_lag_after_stop.csv

5. Stop the worker pool with Ctrl+C, then stop the results processor with Ctrl+C.
6. Generate the report:

       python -m src.load_test.summarize
       Get-Content .\artifacts\load_test\LOAD_TEST_REPORT.md

The summarizer reads `worker_pool.log`, `results_processor.log`, `kafka_lag.csv`, `resources.csv` and (if present) `postgres_benchmark.json`. The post-stop lag sample is a separate drain check and is not automatically merged into the standard report.

## 5. PostgreSQL metadata benchmark

Run this only after the camera simulator, worker pool and results processor have stopped, so it does not compete with the live pipeline.

Rollback-only mode:

    python -m src.load_test.postgres_benchmark --count 2000

Durable commit mode against the local development database:

    python -m src.load_test.postgres_benchmark --count 2000 --commit

Commit mode writes uniquely tagged test rows, measures durable transactions, and removes only rows tagged for that run. Confirm the script reports cleanup success. If it is interrupted before cleanup, review the printed run tag and follow the cleanup instructions in the script before deleting anything.

Regenerate the report after the benchmark:

    python -m src.load_test.summarize

## 6. Database and integrity verification

Check recent persisted rows by model:

    docker exec postgres-postgres-1 psql -U ai_app -d ai_monitoring -c "SELECT model_id, COUNT(*) AS inserted_last_10m, MAX(created_at) AS latest_insert FROM inference_results WHERE created_at >= NOW() - INTERVAL '10 minutes' GROUP BY model_id ORDER BY model_id;"

Run the integrity verification utility if it is present on the branch:

    python -m src.load_test.verify_results_integrity --since-minutes 120

The integrity check should report no duplicate inference records, invalid event-image relationships, orphan event images, or non-event rows linked to images.

## 7. Evaluation criteria

Treat the following as the Phase H planning targets, not pre-certified performance guarantees:

- **Offered source load:** 40 cameras × 8 FPS = 320 unique frames/s.
- **Offered model load:** if the active assignment counts are 7/12/20/17, then car 56, fire 96, person_face 160 and ppe 136 frame records/s (448 total).
- **Worker pool:** exactly 29 worker processes with the configured 4/6/10/9 distribution.
- **Worker throughput:** compare measured aggregate processed FPS per model with that model's verified offered input FPS. Do not infer throughput from the 30 ms delay alone.
- **Kafka lag:** known lag should remain stable or decline during the run; it should drain after camera input stops. Investigate data-bearing partitions with unknown committed offsets. Empty partitions may legitimately have no commit.
- **Results processor:** its measured results throughput must keep pace with the actual combined worker result stream; result-topic lag must not grow continuously.
- **Database:** fresh rows should continue appearing for every model with active camera assignments; integrity verification must pass.
- **Failures and freshness:** review failed counts, dead-letter deliveries and stale-frame skips. A high stale-skip count means the system is not processing the offered stream in time even if lag remains bounded.
- **Resources:** review system CPU/RAM and worker-pool CPU/RAM. As a conservative test safety guardrail, stop and investigate if system RAM remains above 95%, the host becomes unresponsive, or workers/broker/database repeatedly fail. This is a safety guardrail, not a formal production SLO.

The current report contains aggregate worker FPS and stage averages. It does not currently calculate end-to-end latency percentiles or JPEG payload-size percentiles; do not claim those metrics were measured unless separate instrumentation is added.

## 8. What this benchmark does not change

- The `main` branch.
- The four model topics or their partition counts.
- The four results topics or their partition counts.
- Camera-to-model assignments in PostgreSQL.
- The 30 ms mock inference delay.
- The 50% simulated event probability.
- JPEG resolution, quality or event-only image-storage policy.
- The results batching or database schema.

Do not merge `load-test` into `main` until the Phase H results and actual assignment matrix have been reviewed.
