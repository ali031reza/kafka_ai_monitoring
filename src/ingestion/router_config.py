import os
from pathlib import Path
from collections import defaultdict

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]

load_dotenv(ROOT / "infra" / "postgres" / ".env")


def load_camera_routes():
    query = """
        SELECT a.camera_id, m.input_topic
        FROM camera_model_assignments a
        JOIN cameras c ON a.camera_id = c.camera_id
        JOIN models m ON a.model_id = m.model_id
        WHERE a.enabled = TRUE
          AND c.enabled = TRUE
          AND m.enabled = TRUE
        ORDER BY a.camera_id, m.model_id
    """

    routes = defaultdict(list)

    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="ai_monitoring",
        user="ai_app",
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:

        with conn.cursor() as cursor:
            cursor.execute(query)

            for camera_id, topic in cursor:
                routes[camera_id].append(topic)

    return dict(routes)


if __name__ == "__main__":
    routes = load_camera_routes()

    print("Configured cameras:", len(routes))
    print("C01:", routes["C01"])
    print("C11:", routes["C11"])