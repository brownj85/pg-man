import cyclopts
from db_man.lib import schema, db
from db_man import config
import logging
import sys

logging.basicConfig(level=logging.INFO)

app = cyclopts.App("db-man")


@app.command()
def init():
    settings = config.get()

    db_engine = db.connect(settings.db_url)
    with db_engine.connect() as conn:
        schema.init_revisions_table(conn, settings.dbman_schema)
        conn.commit()


@app.command()
def upgrade(*, db_url: str | None = None):
    settings = config.get()
    db_url = db_url or settings.db_url

    repo = schema.RevisionRepo(
        settings.revision_dir, dbman_schema=settings.dbman_schema
    )
    if not repo.revisions:
        print(f"No revision files in directory '{repo.root}'")

    db_engine = db.connect(settings.db_url)
    with db_engine.connect() as conn:
        repo.upgrade_db(conn)
        conn.commit()


@app.command()
def revision(name: str, *, autogenerate: bool = True):
    settings = config.get()
    rev_repo = schema.RevisionRepo(
        settings.revision_dir, dbman_schema=settings.dbman_schema
    )

    with db.connect(settings.db_url).connect() as conn:
        if rev_repo.get_current_revision(conn) != rev_repo.head:
            print("Database is not up to date")
            sys.exit(1)

    ddl_repo = schema.DDLRepo(settings.ddl_dir)
    if autogenerate:
        content = schema.generate_revision(settings, ddl_repo, rev_repo)
    else:
        content = ""

    rev = rev_repo.add(name, content)

    print(f"Created new revision {rev.path}")
