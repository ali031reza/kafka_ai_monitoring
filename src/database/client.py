from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import psycopg


class DatabaseClient:
    """
    Manages PostgreSQL database connections and transactions.

    Responsibilities:
        - Establish database connections
        - Execute SQL queries
        - Fetch database records
        - Manage transaction boundaries
        - Commit successful transactions
        - Roll back failed transactions
    """

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
    ):

        self._config = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        """
        Provide a transactional database connection.

        Successful execution:
            COMMIT

        Failed execution:
            ROLLBACK

        The connection is closed automatically.
        """

        with psycopg.connect(**self._config) as connection:
            yield connection

    def fetch_all(
        self,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> list[tuple]:

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(query, params or ())

                return cursor.fetchall()

    def fetch_one(
        self,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> tuple | None:

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(query, params or ())

                return cursor.fetchone()

    def execute(
        self,
        query: str,
        params: Sequence[Any] | None = None,
    ) -> int:
        """
        Execute an SQL statement.

        Returns:
            Number of affected rows.
        """

        with self.transaction() as connection:
            with connection.cursor() as cursor:

                cursor.execute(query, params or ())

                return cursor.rowcount