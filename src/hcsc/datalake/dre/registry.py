"""Registries of the engine's capabilities (spec section 2, "Extension points").

Checks, cause probes, builtin causes, gold rule templates and key normalisers each have one
registry. A capability registers itself in the module that defines it; nothing central lists
them. The built-in ones are found by importing the engine's own packages on first use
(`discover`), so adding one is adding a module. An id registered twice is an error: ids go
into evaluation_id and cause rows, so they must be unique and stable.
"""

from __future__ import annotations

import importlib
import pkgutil
from contextlib import contextmanager
from typing import Generic, Iterator, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    def __init__(self, kind: str, discover: tuple[str, ...] = ()) -> None:
        self.kind = kind
        self._discover = discover  # packages (or modules) whose import registers the built-ins
        self._items: dict[str, T] = {}
        self._loaded = False

    def register(self, item_id: str, item: T) -> T:
        if not item_id:
            raise ValueError(f"a {self.kind} needs an id")
        if item_id in self._items:
            raise ValueError(f"{self.kind} {item_id!r} is registered twice")
        self._items[item_id] = item
        return item

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        for name in self._discover:
            module = importlib.import_module(name)
            path = getattr(module, "__path__", None)
            if path is not None:
                for info in pkgutil.walk_packages(path, prefix=module.__name__ + "."):
                    importlib.import_module(info.name)

    def __getitem__(self, item_id: str) -> T:
        self._load()
        try:
            return self._items[item_id]
        except KeyError:
            raise KeyError(f"unknown {self.kind} {item_id!r}; known: {self.ids()}") from None

    def unregister(self, item_id: str) -> None:
        """Remove a registration (configuration-defined capabilities are replaced on each load)."""
        self._items.pop(item_id, None)

    def get(self, item_id: str) -> T | None:
        self._load()
        return self._items.get(item_id)

    def __contains__(self, item_id: object) -> bool:
        self._load()
        return item_id in self._items

    def ids(self) -> tuple[str, ...]:
        self._load()
        return tuple(sorted(self._items))

    def values(self) -> tuple[T, ...]:
        self._load()
        return tuple(self._items[i] for i in sorted(self._items))

    @contextmanager
    def temporary(self, item_id: str, item: T) -> Iterator[T]:
        """Register for the duration of a block (tests)."""
        self._load()
        self.register(item_id, item)
        try:
            yield item
        finally:
            del self._items[item_id]
