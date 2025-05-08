import re
import yaml
from dataclasses import dataclass
from typing import Any, TextIO
import io

_FRONT_MATTER_RE = re.compile(
    r"^\/\*\s*((?P<start>^[-]{3,}$)(?P<front_matter>[\s\S]*)(?P=start))?(?P<doc>[\s\S]*)\*\/",
    flags=re.MULTILINE,
)


@dataclass
class FrontMatter:
    data: dict[str, Any]
    doc: str

    @classmethod
    def parse(cls, sql: str) -> tuple["FrontMatter | None", str]:
        m = _FRONT_MATTER_RE.search(sql)
        if not m:
            return (None, sql)

        if front_matter_yaml := m.group("front_matter"):
            data = yaml.load(front_matter_yaml, yaml.SafeLoader)
        else:
            data = {}

        doc = m.group("doc").strip() or ""

        rest = sql[m.end() :]

        return FrontMatter(data=data, doc=doc), rest

    def dump(self, outfile: TextIO) -> None:
        outfile.write("/*\n")
        outfile.write("---\n")
        yaml.dump(self.data, indent=2)
        outfile.write("---\n")
        outfile.write(self.doc)
        outfile.write("*/\n")

    def dumps(self) -> str:
        with io.StringIO() as out:
            self.dump(out)

            return out.getvalue()
