-- ==========================================
-- EVENT IMAGES
-- ==========================================

CREATE TABLE IF NOT EXISTS event_images (

    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    camera_id VARCHAR(50) NOT NULL,

    frame_id VARCHAR(100) NOT NULL,

    storage_key TEXT NOT NULL UNIQUE,

    image_format VARCHAR(10) NOT NULL DEFAULT 'jpeg',

    file_size_bytes BIGINT NOT NULL
        CHECK (file_size_bytes > 0),

    sha256 CHAR(64) NOT NULL,

    captured_at_ms BIGINT NOT NULL,

    stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_event_camera_frame
        UNIQUE (camera_id, frame_id)
);


-- ==========================================
-- INFERENCE RESULTS
-- ==========================================

CREATE TABLE IF NOT EXISTS inference_results (

    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    camera_id VARCHAR(50) NOT NULL,

    frame_id VARCHAR(100) NOT NULL,

    model_id VARCHAR(50) NOT NULL,

    processed_at_ms BIGINT NOT NULL,

    status VARCHAR(50) NOT NULL,

    is_event BOOLEAN NOT NULL DEFAULT FALSE,

    event_type VARCHAR(100),

    event_image_id BIGINT REFERENCES event_images(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- Support an existing inference_results table
ALTER TABLE inference_results
    ADD COLUMN IF NOT EXISTS is_event BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE inference_results
    ADD COLUMN IF NOT EXISTS event_type VARCHAR(100);

ALTER TABLE inference_results
    ADD COLUMN IF NOT EXISTS event_image_id BIGINT
    REFERENCES event_images(id);


-- ==========================================
-- INDEXES
-- ==========================================

CREATE UNIQUE INDEX IF NOT EXISTS
    uq_inference_results_model_frame
    ON inference_results (model_id, frame_id);

CREATE INDEX IF NOT EXISTS
    idx_event_images_camera_time
    ON event_images (camera_id, captured_at_ms DESC);

CREATE INDEX IF NOT EXISTS
    idx_inference_results_model_time
    ON inference_results (model_id, processed_at_ms DESC);

CREATE INDEX IF NOT EXISTS
    idx_inference_results_events
    ON inference_results (is_event, processed_at_ms DESC);