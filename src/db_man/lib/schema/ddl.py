from dataclasses import dataclass
from pathlib import PurePosixPath, Path
from typing import TypeAlias
from collections.abc import Mapping
from db_man.lib.front_matter import FrontMatter
import logging
from pydantic import BaseModel, ConfigDict
from contextlib import contextmanager
import sqlalchemy as sa
from db_man.lib import sort

logger = logging.getLogger("dbman")

RepoPath: TypeAlias = PurePosixPath


class DDLFileConfig(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )

    depends_on: frozenset[RepoPath] = frozenset()


@dataclass
class DDLFile:
    path: Path
    config: DDLFileConfig
    doc: str
    depends_on: frozenset["DDLFile"] = frozenset()

    @contextmanager
    def open(self):
        with open(self.path, "r") as f:
            yield f

    @property
    def content(self) -> str:
        with self.open() as f:
            return f.read()

    def __hash__(self):
        return hash(self.path)


class DDLRepo:
    root: Path

    def __init__(self, root: Path | str):
        self.root = Path(root).resolve()
        self._files = {}
        self._topological_order: list[DDLFile] | None = None

        for spath in self.root.rglob("*.sql"):
            self._load_one(spath)

    @property
    def files(self) -> Mapping[Path, DDLFile]:
        return self._files

    @property
    def topological_order(self):
        if self._topological_order is None:
            self._topological_order = list(
                sort.topological_sort(self.files.values(), lambda f: f.depends_on)
            )

        yield from self._topological_order

    def apply(self, conn: sa.Connection):
        for ddl in self.topological_order:
            conn.execute(sa.text(ddl.content))

    def _load_one(self, real_path: Path) -> DDLFile:
        real_path = real_path.resolve()
        if resolved := self.files.get(real_path):
            return resolved

        self._topological_order = None

        with open(real_path, "r") as f:
            sql = f.read()

        fm, _ = FrontMatter.parse(sql)
        if fm is None:
            config = DDLFileConfig()
            doc = ""
        else:
            config = DDLFileConfig.model_validate(fm.data)
            doc = fm.doc

        dep_paths: list[Path] = []
        for dep_path in config.depends_on:
            if dep_path.is_absolute():
                dep_real_path = self.root / dep_path.relative_to(RepoPath("/"))
            else:
                dep_real_path = Path(real_path.parent, dep_path)

            if dep_real_path.is_file():
                dep_paths.append(dep_real_path)
            else:
                dep_paths.extend(p for p in dep_real_path.rglob("*.sql") if p.is_file())

        depends_on: list[DDLFile] = [self._load_one(p) for p in dep_paths]

        ddl_file = DDLFile(
            path=real_path,
            config=config,
            doc=doc,
            depends_on=frozenset(depends_on),
        )
        self._files[real_path] = ddl_file

        return ddl_file
