from db_man.lib import db, pg
from db_man.lib.schema import ddl, revisions
from db_man.config import Settings
import tempfile
import subprocess
from pathlib import Path


def generate_revision(
    settings: Settings, ddl_repo: ddl.DDLRepo, revisions_repo: revisions.RevisionRepo
) -> str:
    db_engine = db.connect(settings.db_url)
    conn = db_engine.connect()

    curr_revision = revisions_repo.get_current_revision(conn)
    if curr_revision != revisions_repo.head:
        raise RuntimeError("Database not up to date")

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        pg.PostgresProcess(settings.postgres_path) as pg_proc,
    ):
        ddl_db_engine = db.connect(pg_proc.url())
        with ddl_db_engine.connect() as conn:
            ddl_repo.apply(conn)
            conn.commit()

        ddl_db_engine.dispose()

        current_path = Path(tmpdir, "current.sql")
        upgrade_path = Path(tmpdir, "upgrade.sql")
        subprocess.run(
            [
                "pg_dump",
                "--no-owner",
                "--schema-only",
                "--exclude-schema",
                settings.dbman_schema,
                "--file",
                str(current_path),
                settings.db_url,
            ]
        ).check_returncode()
        subprocess.run(
            [
                "pg_dump",
                "--no-owner",
                "--schema-only",
                "--exclude-schema",
                settings.dbman_schema,
                "--file",
                str(upgrade_path),
                pg_proc.url(),
            ]
        ).check_returncode()

        result = subprocess.run(
            [
                "java",
                "-jar",
                settings.apgdiff_jar_path,
                str(current_path),
                str(upgrade_path),
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        result.check_returncode()

        return result.stdout
