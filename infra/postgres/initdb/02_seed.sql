-- Initial development data. 40 cameras, four AI services, 56 assignments.
-- Kafka topic names follow the already agreed model_1..model_4 pattern.

INSERT INTO models (model_id, name, input_topic, result_topic) VALUES
  ('car',         'Car Detection',         'model_1.frames', 'model_1.results'),
  ('fire',        'Fire Detection',        'model_2.frames', 'model_2.results'),
  ('person_face', 'Person/Face Detection', 'model_3.frames', 'model_3.results'),
  ('ppe',         'PPE Detection',         'model_4.frames', 'model_4.results')
ON CONFLICT (model_id) DO NOTHING;

-- RTSP URLs are intentionally NULL: real endpoints must be added later.
INSERT INTO cameras (camera_id, name)
SELECT 'C' || LPAD(n::text, 2, '0'), 'Camera ' || LPAD(n::text, 2, '0')
FROM generate_series(1, 40) AS s(n)
ON CONFLICT (camera_id) DO NOTHING;

-- C01-C04 : fire + PPE + person/face (4 triple-overlap cameras)
-- C05-C10 : PPE + person/face (6 other PPE-person overlaps)
-- C11-C12 : car + person/face (2 car-person overlaps)
-- C13-C20 : person/face only
-- C21-C25 : PPE only
-- C26-C31 : fire only
-- C32-C36 : car only
-- C37-C38 : fire only
-- C39-C40 : PPE only
WITH assignment_rows AS (
    SELECT 'C' || LPAD(n::text, 2, '0') AS camera_id,
           UNNEST(
               CASE
                   WHEN n BETWEEN  1 AND  4 THEN ARRAY['fire', 'ppe', 'person_face']
                   WHEN n BETWEEN  5 AND 10 THEN ARRAY['ppe', 'person_face']
                   WHEN n BETWEEN 11 AND 12 THEN ARRAY['car', 'person_face']
                   WHEN n BETWEEN 13 AND 20 THEN ARRAY['person_face']
                   WHEN n BETWEEN 21 AND 25 THEN ARRAY['ppe']
                   WHEN n BETWEEN 26 AND 31 THEN ARRAY['fire']
                   WHEN n BETWEEN 32 AND 36 THEN ARRAY['car']
                   WHEN n BETWEEN 37 AND 38 THEN ARRAY['fire']
                   WHEN n BETWEEN 39 AND 40 THEN ARRAY['ppe']
               END
           ) AS model_id
    FROM generate_series(1, 40) AS s(n)
)
INSERT INTO camera_model_assignments (camera_id, model_id)
SELECT camera_id, model_id FROM assignment_rows WHERE TRUE
ON CONFLICT (camera_id, model_id) DO NOTHING;
