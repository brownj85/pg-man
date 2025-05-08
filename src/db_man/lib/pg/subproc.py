import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self
from uuid import uuid4

import sqlalchemy


class PostgresProcess:
    def __init__(
        self,
        postgres_path: Path,
        user: str = "postgres",
    ):
        self.postgres_path = postgres_path
        self.user = user

        self._proc: subprocess.Popen | None = None
        self._tmpdir: TemporaryDirectory | None = None

    @property
    def proc(self) -> subprocess.Popen:
        if self._proc is None:
            raise RuntimeError("Not started")

        return self._proc

    @property
    def tmpdir(self) -> Path:
        if self._tmpdir is None:
            raise RuntimeError("Not started")

        return Path(self._tmpdir.name).absolute()

    def url(self) -> str:
        return f"postgresql://{self.user}@/postgres?host={str(self.tmpdir)}"

    @property
    def host(self) -> str:
        return str(self.tmpdir)

    def start(self):
        if self._proc is not None:
            raise RuntimeError("Already started")

        self._tmpdir = TemporaryDirectory()

        subprocess.run(
            (
                str(self.postgres_path / "bin" / "initdb"),
                "-D",
                str(self.tmpdir / "data"),
                "--username",
                "postgres",
                "--auth-local",
                "trust",
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ).check_returncode()

        self._proc = subprocess.Popen(
            (
                str(self.postgres_path / "bin" / "postgres"),
                "-k",
                str(self.tmpdir),
                "-D",
                str(self.tmpdir / "data"),
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        ready = False
        ttl = 5
        while not ready and ttl > 0:
            ret = subprocess.run(
                (
                    str(self.postgres_path / "bin" / "pg_isready"),
                    "--username",
                    "postgres",
                    "--dbname",
                    "postgres",
                    "-t",
                    str(1),
                    "-h",
                    str(self.tmpdir),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if ret.returncode != 0:
                ttl -= 1
                if self._proc.poll():
                    self.stop()
                    raise RuntimeError(
                        f"Failed to start postgres (exit code {self._proc.returncode}): {self._proc.stdout.read()}"
                    )
                time.sleep(0.1)
            else:
                ready = True

        if not ready:
            self.stop()
            raise RuntimeError("Postgres instance failed to start")

    def stop(self):
        if self._proc is None:
            return

        self._proc.kill()
        self._proc.wait(5)
        self._proc = None
        self._tmpdir.cleanup()
        self._tmpdir = None

    def __enter__(self):
        self.start()

        return self

    def __exit__(self, *args):
        self.stop()


class TemporaryDatabase:
    def __init__(
        self,
        base_url: sqlalchemy.URL,
        name: str | None = None,
        template_name: str | None = None,
        is_template: bool = False,
    ):
        self.base_url = base_url
        self.name = name or f"tempdb_{str(uuid4()).replace('-', '')}"
        self.template_name = template_name
        self.is_template = is_template

    def url(self, dialect: str | None = None) -> sqlalchemy.URL:
        url = self.base_url.set(database=self.name)

        if dialect:
            url = url.set(drivername=f"postgresql+{dialect}")

        return url

    def create(self):
        engine = sqlalchemy.create_engine(self.base_url, poolclass=sqlalchemy.NullPool)
        conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")

        cmd = f"CREATE DATABASE {self.name}"
        withs = ""
        if self.template_name:
            withs += f" TEMPLATE {self.template_name}"

        if withs:
            cmd += f" WITH {withs}"

        conn.execute(sqlalchemy.text(cmd))
        conn.close()
        engine.dispose()

    def destroy(self):
        engine = sqlalchemy.create_engine(self.base_url, poolclass=sqlalchemy.NullPool)
        conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(
            sqlalchemy.text(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{self.name}'
            AND pid <> pg_backend_pid();
        """)
        )
        conn.execute(sqlalchemy.text(f"DROP DATABASE {self.name}"))
        conn.close()
        engine.dispose()

    def connect(
        self,
        dialect: str = "psycopg",
        *,
        poolclass: type[sqlalchemy.Pool] = sqlalchemy.NullPool,
        **create_engine_kwargs: Any,
    ) -> sqlalchemy.Engine:
        return sqlalchemy.create_engine(
            self.url(dialect=dialect), poolclass=poolclass, **create_engine_kwargs
        )

    def __enter__(self) -> Self:
        self.create()
        return self

    def __exit__(self, *_):
        self.destroy()
