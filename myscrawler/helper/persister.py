"""
SQLite Persister.
"""

import sqlite3

from pathlib import Path

from . import io_helper


class Persister:
    """
    SQLite file manipulator

    Usage:

    - init a new sqlite instance

    ```python
        per = Persister(
            file="./temp/sqlite_test.db",
            check="select 1 from test order by rowid asc limit 1",
            ddl="create table test (a primary key, b, c)",
        )
    ```

    - insert a new record

    ```python
        per.execute(
            "insert into test values(CURRENT_TIMESTAMP, ?, ?)",
            ("new b", "new c")
        )
    ```

    - select one record

    ```python
        print(per.fetchone("select * from test order by rowid desc limit 1"))
    ```

    - close connector

    ```python
        per.close()
    ```
    """

    def __init__(self, file, check, ddl):
        """Init connector and ensure ddl exists"""
        # print(file, check, ddl)
        io_helper.ensure_path(Path(file).parent)
        self.__db = sqlite3.connect(file)
        self.ensure(check, ddl)

    def ensure(self, check, ddl):
        """Ensure ddl exists"""
        cursor = self.__db.cursor()
        try:
            cursor.execute(check)
        except sqlite3.OperationalError:
            cursor.execute(ddl)
        self.__db.commit()

    def fetchone(self, sql, val=()):
        """Fetch one record using specified sql"""
        cursor = self.__db.cursor()
        cursor.execute(sql, val)
        return cursor.fetchone()

    def execute(self, sql, val=()):
        """Insert a record using specified sql"""
        cursor = self.__db.cursor()
        try:
            cursor.execute(sql, val)
            self.__db.commit()
        except Exception as e:
            self.__db.rollback()
            raise e

    def close(self):
        """Close connector"""
        self.__db.close()
