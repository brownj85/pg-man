import sqlalchemy as sa
from logging import getLogger

logger = getLogger("db-man")


def connect(db_url: str) -> sa.Engine:
    uri = sa.make_url(db_url)
    uri = uri.set(drivername="postgresql+psycopg")

    return sa.create_engine(uri, poolclass=sa.NullPool)
