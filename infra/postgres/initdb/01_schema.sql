-- Database: ai_monitoring
-- Cameras are sources of frames; models are inference destinations.
-- A camera can be assigned to multiple models and vice versa.

CREATE TABLE IF NOT EXISTS cameras (
    camera_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    rtsp_url        TEXT,
    credential_ref  TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Do not embed RTSP passwords in rtsp_url or store credentials in Git.
-- credential_ref may point to a secret managed outside the database.

CREATE TABLE IF NOT EXISTS models (
    model_id        TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    input_topic     TEXT NOT NULL UNIQUE,
    result_topic    TEXT NOT NULL UNIQUE,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS camera_model_assignments (
    camera_id       TEXT NOT NULL REFERENCES cameras(camera_id) ON DELETE CASCADE,
    model_id        TEXT NOT NULL REFERENCES models(model_id) ON DELETE CASCADE,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    assigned_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (camera_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_assignments_model_id
    ON camera_model_assignments (model_id);
