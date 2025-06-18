from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence, Mapping
from contextlib import contextmanager
import parse
import functools
import sqlalchemy as sa
import logging
from pg_man.lib.uid import short_uid

logger = logging.getLogger("db_man.revisions")

REVISION_FILENAME_FMT = "{index:04d}_{uid}_{name}.sql"


@dataclass(frozen=True)
class Revision:
    index: int
    uid: str
    name: str
    path: Path

    @contextmanager
    def open(self):
        with open(self.path, "r") as f:
            yield f

    @property
    def content(self) -> str:
        with self.open() as f:
            return f.read()

    def __lt__(self, other: "Revision | None") -> bool:
        if other is None:
            return False

        return (self.index, self.uid) < (other.index, other.uid)

    def __eq__(self, other: "Revision | None") -> bool:
        if other is None:
            return False
        return (self.index, self.uid) == (other.index, other.uid)


class RevisionRepo:
    root: Path

    def __init__(self, root: Path, dbman_schema: str):
        self.root = root
        self.dbman_schema = dbman_schema
        self._revisions: list[Revision] = []
        self._revisions_by_name: dict[str, Revision] = {}

        self.load()

    def load(self):
        revisions = []
        by_name = {}

        fname_parser = _revision_filename_parser()
        for rev_file in self.root.glob("*.sql"):
            parsed_fname: parse.Result | None = fname_parser.parse(
                rev_file.name, evaluate_result=True
            )
            if parsed_fname is None:
                raise RuntimeError(f"Invalid revision filename: ${rev_file}")

            rev = Revision(
                path=rev_file.resolve(),
                index=parsed_fname["index"],
                uid=parsed_fname["uid"],
                name=parsed_fname["name"],
            )

            revisions.append(rev)
            by_name[rev.name] = rev

        revisions.sort()

        self._revisions = revisions
        self._revisions_by_name = by_name

    def add(self, name: str, content: str = "") -> Revision:
        self.load()

        index = len(self._revisions)
        uid = short_uid()
        if not self.root.exists():
            self.root.mkdir()

        with open(
            self.root / REVISION_FILENAME_FMT.format(index=index, name=name, uid=uid),
            "w",
        ) as f:
            f.write(content)

        self.load()
        return self.revisions[-1]

    @property
    def revisions(self) -> Sequence[Revision]:
        return self._revisions

    @property
    def revisions_by_name(self) -> Mapping[str, Revision]:
        return self._revisions_by_name

    @property
    def head(self) -> Revision | None:
        if self.revisions:
            return self.revisions[-1]

        return None

    def upgrade_db(self, conn: sa.Connection):
        if (revisions_table := get_revisions_table(conn, self.dbman_schema)) is None:
            revisions_table = init_revisions_table(conn, dbman_schema=self.dbman_schema)

        curr = self.get_current_revision(conn)
        if curr is None:
            index = 0
        else:
            index = curr.index + 1

        for rev in self.revisions[index:]:
            apply_revision(conn, revisions_table, rev)
            logger.info("Applied revision %s", rev.path.name)

    def get_current_revision(self, conn: sa.Connection) -> Revision | None:
        rev_table = _get_revisions_table_if_exists(conn, self.dbman_schema)

        if rev_table is None:
            return None

        if not (row := get_current_revision(conn, rev_table)):
            return None

        index, uid, name = row
        filename = REVISION_FILENAME_FMT.format(index=index, uid=uid, name=name)
        if not (path := self.root / filename).exists():
            raise RuntimeError(f"Can't locate head revision: {path}")

        return self.revisions[index]


def get_revisions_table(conn: sa.Connection, dbman_schema: str) -> sa.Table | None:
    if not conn.dialect.has_table(
        conn, _revisions_table(dbman_schema).name, schema=dbman_schema
    ):
        return None

    return _revisions_table(dbman_schema)


def init_revisions_table(conn: sa.Connection, dbman_schema: str) -> sa.Table:
    rev_table = _revisions_table(dbman_schema)

    if not conn.dialect.has_schema(conn, dbman_schema):
        conn.execute(sa.text(f"CREATE SCHEMA {dbman_schema};"))
        logger.info("Created revisions schema: '%s'", dbman_schema)

    rev_table.create(conn)
    logger.info("Created revisions table: '%s'", rev_table.fullname)

    return rev_table


def apply_revision(conn: sa.Connection, revisions_table: sa.Table, revision: Revision):
    conn.execute(sa.text(revision.content))
    conn.execute(
        sa.insert(revisions_table).values(
            index=revision.index, uid=revision.uid, name=revision.name
        )
    )


def get_current_revision(
    conn: sa.Connection, revisions_table: sa.Table
) -> tuple[int, str, str] | None:
    return (
        conn.execute(
            sa.select(
                revisions_table.c.index, revisions_table.c.uid, revisions_table.c.name
            )
            .order_by(revisions_table.c.index.desc())
            .limit(1)
            .with_for_update()
        )
        .tuples()
        .one_or_none()
    )


def _get_revisions_table_if_exists(
    conn: sa.Connection, dbman_schema: str
) -> sa.Table | None:
    table = _revisions_table(dbman_schema)
    if conn.dialect.has_table(conn, table.name, schema=table.schema):
        return table

    return None


@functools.cache
def _revision_filename_parser() -> parse.Parser:
    return parse.compile(REVISION_FILENAME_FMT)


@functools.cache
def _revisions_table(dbman_schema: str) -> sa.Table:
    metadata = sa.MetaData(schema=dbman_schema)

    return sa.Table(
        "current_revision",
        metadata,
        sa.Column("index", sa.INTEGER(), nullable=False, primary_key=True),
        sa.Column("uid", sa.TEXT(), nullable=False, unique=True),
        sa.Column("name", sa.TEXT(), nullable=False),
    )
