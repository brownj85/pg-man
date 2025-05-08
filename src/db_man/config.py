from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Annotated
import functools


class Settings(BaseSettings):
    apgdiff_jar_path: Path = Path("apgdiff-2.7.0.jar")
    workdir: Annotated[Path, Field(alias="DBMAN_WORKDIR")] = Path("schema")
    dbman_schema: str = "dbman"
    db_url: str
    managed_schemas: set[str] = {"people", "finance"}
    postgres_path: Path = Path("/usr/lib/postgresql/16")

    @property
    def ddl_dir(self) -> Path:
        return self.workdir / "ddl"

    @property
    def revision_dir(self) -> Path:
        return self.workdir / "revisions"


@functools.cache
def get() -> Settings:
    return Settings()
