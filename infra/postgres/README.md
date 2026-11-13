# AI Monitoring — PostgreSQL starter

This is the **database-only** first step for the 40-camera, four-model Kafka project.
It does **not** install Kafka or connect to cameras yet.

## 1. Start PostgreSQL

Requirements: Docker with Docker Compose.

```bash
cp .env.example .env
# Edit .env and replace the password placeholder before starting.
docker compose up -d
```

The container creates database `ai_monitoring` using SQL in `initdb/` on the **first** startup with an empty data volume.
A persistent Docker volume keeps your data across restarts.

## 2. Inspect the database

```bash
docker compose exec postgres psql -U ai_app -d ai_monitoring
```

In the `psql` prompt:

```sql
\dt
SELECT COUNT(*) AS cameras FROM cameras;  -- 40
SELECT COUNT(*) AS assignments FROM camera_model_assignments;  -- 56
SELECT COUNT(DISTINCT camera_id) AS assigned_cameras
FROM camera_model_assignments WHERE enabled;  -- 40

SELECT m.model_id, m.name, COUNT(a.camera_id) AS assigned_cameras,
       12 * COUNT(a.camera_id) AS incoming_fps
FROM models m
LEFT JOIN camera_model_assignments a
       ON a.model_id = m.model_id AND a.enabled
GROUP BY m.model_id, m.name
ORDER BY m.model_id;
-- car: 7 cameras / 84 FPS
-- fire: 12 cameras / 144 FPS
-- person_face: 20 cameras / 240 FPS
-- ppe: 17 cameras / 204 FPS
```

Exit `psql` with `\q`.

## 3. Update an actual camera

Replace the sample address below with the real camera endpoint. Do not include a password in the URL.

```sql
UPDATE cameras
SET rtsp_url = 'rtsp://192.0.2.10:554/stream1',
    credential_ref = 'camera/C01'
WHERE camera_id = 'C01';
```

The example address is a documentation-only placeholder, not a live camera.

## 4. Read the routes in your ingestion service

```sql
SELECT a.camera_id, m.model_id, m.input_topic
FROM camera_model_assignments a
JOIN cameras c ON c.camera_id = a.camera_id
JOIN models m ON m.model_id = a.model_id
WHERE a.enabled AND c.enabled AND m.enabled
ORDER BY a.camera_id, m.model_id;
```

Your ingestion service should **cache** these routes rather than run this query for each frame. We will design how cache refresh works in the next step.

## 5. Change an assignment

```sql
-- Add a model to one camera (if not already assigned):
INSERT INTO camera_model_assignments (camera_id, model_id)
VALUES ('C21', 'fire')
ON CONFLICT (camera_id, model_id) DO UPDATE SET enabled = TRUE;

-- Disable that assignment without deleting it:
UPDATE camera_model_assignments
SET enabled = FALSE
WHERE camera_id = 'C21' AND model_id = 'fire';
```

## Notes

- Kafka input load from these mappings: `56 × 12 = 672` frame records/second (before any camera is disabled).
- `model_1.frames` = Car, `model_2.frames` = Fire, `model_3.frames` = Person/Face, and `model_4.frames` = PPE.
- Keep `.env` out of Git; it contains the database password.
- This single-host configuration is intended for initial development. It is not highly available.
- The `initdb` scripts do **not** re-run on every `docker compose up`; they run only when the PostgreSQL data directory is first initialized.
- `docker compose down` stops services but preserves the volume. **`docker compose down -v` deletes your database data** and should not be used casually.
