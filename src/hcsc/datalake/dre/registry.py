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
    def __init__(self, label: str, discover: tuple[str, ...] = ()) -> None:
        self.label = label  # what an item is called in messages: "check", "probe", ...
        self._discover = discover  # packages (or modules) whose import registers the built-ins
        self._items: dict[str, T] = {}
        self._loaded = False
        self._conf_ids: set[str] = set()  # ids registered from configuration, replaced on each load

    def register(self, item_id: str, item: T) -> T:
        if not item_id:
            raise ValueError(f"a {self.label} needs an id")
        if item_id in self._items:
            raise ValueError(f"{self.label} {item_id!r} is registered twice")
        self._items[item_id] = item
        return item

    def _load(self) -> None:
        """Import the packages that register the built-ins, once. A failing import is raised to the
        caller and tried again on the next use, never left as a silently partial registry."""
        if self._loaded:
            return
        for name in self._discover:
            module = importlib.import_module(name)
            path = getattr(module, "__path__", None)
            if path is not None:
                for info in pkgutil.walk_packages(path, prefix=module.__name__ + "."):
                    importlib.import_module(info.name)
        self._loaded = True

    def __getitem__(self, item_id: str) -> T:
        self._load()
        try:
            return self._items[item_id]
        except KeyError:
            raise KeyError(f"unknown {self.label} {item_id!r}; known: {self.ids()}") from None

    def unregister(self, item_id: str) -> None:
        """Remove a registration (an extension unloaded, or a test's temporary item)."""
        self._items.pop(item_id, None)
        self._conf_ids.discard(item_id)

    def replace_conf(self, items: dict[str, T]) -> list[str]:
        """Register the capabilities a configuration defines, replacing the previous conf set. An
        id that code already holds (packaged, or from an extension) is refused and returned."""
        self._load()
        for old in self._conf_ids:
            self._items.pop(old, None)
        self._conf_ids.clear()
        refused = []
        for item_id, item in items.items():
            if item_id in self._items:
                refused.append(item_id)
                continue
            self._items[item_id] = item
            self._conf_ids.add(item_id)
        return refused

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
