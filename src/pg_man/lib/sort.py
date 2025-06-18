from collections.abc import Hashable, Iterable, Callable, Iterator
from typing import TypeVar, Any

_T = TypeVar("_T", bound=Hashable)


class CycleError(ValueError):
    node: Any

    def __init__(self, node: Any):
        self.node = node
        super().__init__(f"Node {repr(node)} depends on itself.")


def topological_sort(
    nodes: Iterable[_T],
    get_deps: Callable[[_T], Iterable[_T] | None],
    *,
    memo: set[_T] | None = None,
) -> Iterator[_T]:
    memo = memo if memo is not None else set()
    for node in nodes:
        if node in memo:
            continue

        if (dep_iter := get_deps(node)) is not None:
            for dep in dep_iter:
                if hash(dep) == hash(node):
                    raise CycleError(node)

            yield from topological_sort(dep_iter, get_deps, memo=memo)

        memo.add(node)

        yield node
