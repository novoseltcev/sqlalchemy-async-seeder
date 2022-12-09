from importlib import import_module
from inspect import isclass, ismodule
from types import ModuleType

from sqlalchemy import inspect
from sqlalchemy.exc import NoInspectionAvailable


class ClassRegistry(object):
    """A cache of mappable classes"""

    def __init__(self) -> None:
        self.class_path_cache: dict[str, type] = {}

    def __getitem__(self, item: str) -> type:
        if ":" not in item:
            for cls in self.registered_classes:
                if cls.__name__ == item:
                    return cls
            raise AttributeError("No registered class found for '{}'".format(item))

        if item in self.class_path_cache:
            return self.class_path_cache[item]
        result = self.register(item)
        return result.pop() if isinstance(result, set) else result

    @property
    def registered_classes(self) -> set[type]:
        return set(self.class_path_cache.values())

    def register(self, target: str | type | object) -> set[type] | type:
        if isinstance(target, str):
            target = self.parse_to_target(source=target)

        if isclass(target):
            return self.register_class(target)
        if ismodule(target):
            return self.register_module(target)
        raise ValueError(
            "Cannot register target of type '{}'".format(type(target).__name__)
        )

    @classmethod
    def parse_to_target(cls, source: str) -> ModuleType | type:
        names = source.split(":", 1)
        module_name = names.pop(0)
        module = import_module(module_name)
        if not names:
            return module

        try:
            class_name = names.pop(0)
            return type(getattr(module, class_name))
        except AttributeError:
            raise ValueError(
                "No class '{}' found in module '{}'".format(names[1], module_name)
            )

    def register_class(self, cls: type) -> type:
        if not self._is_mappable(cls):
            raise ValueError(
                "Class {} does not have an associated mapper.".format(cls.__name__)
            )
        self.class_path_cache[cls.__module__ + ":" + cls.__name__] = cls
        return cls

    def register_module(self, module_: ModuleType) -> set[type]:
        mappable_classes = filter(
            self._is_mappable,
            (
                getattr(module_, attr)
                for attr in dir(module_)
                if not attr.startswith("_")
            ),
        )
        for cls in mappable_classes:
            self.register_class(cls)
        return set(mappable_classes)

    @staticmethod
    def _is_mappable(cls: type) -> bool:
        try:
            return isclass(cls) and inspect(cls).mapper
        except NoInspectionAvailable:
            return False
