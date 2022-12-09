from collections import defaultdict
from types import ModuleType
from typing import Type

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import ClassRegistry
from .resolver import ReferenceResolver, Seed
from .schema import SeedGroup


class BaseSeeder:
    def __init__(
        self,
        session: AsyncSession,
        schema: Type[BaseModel] = SeedGroup,
        registry: ClassRegistry = ClassRegistry(),
    ) -> None:
        self.session = session
        self.schema = schema
        self.registry = registry

    async def load_list(
        self,
        seed: Seed | list[Seed],
        flush_on_create: bool = True,
        commit: bool = False,
    ) -> list[object]:
        self.__validate_seed(seed)
        resolver = ReferenceResolver(
            session=self.session,
            registry=self.registry,
            flush_on_create=flush_on_create,
        )
        generated_entities = await resolver.generate_entities(seed)
        if commit:
            await self.session.commit()
        return generated_entities

    async def load_map(
        self,
        seed: Seed | list[Seed],
        flush_on_create: bool = True,
        commit: bool = False,
    ) -> dict[type, list[object]]:
        self.__validate_seed(seed)
        resolver = ReferenceResolver(
            session=self.session,
            registry=self.registry,
            flush_on_create=flush_on_create,
        )

        generated_entities = await resolver.generate_entities(seed)
        if commit:
            await self.session.commit()

        entity_dict = defaultdict(list)
        for e in generated_entities:
            entity_dict[e.__class__].append(e)
        return entity_dict

    def __validate_seed(self, seed: Seed | list[Seed]) -> None:
        if isinstance(seed, list):
            for group in seed:
                self.schema(**group.__dict__)
        else:
            self.schema(**seed.__dict__)

    def register(self, target: str | object | type) -> type | set[type]:
        return self.registry.register(target)

    def register_class(self, target: type) -> type:
        return self.registry.register_class(target)

    def register_module(self, target: ModuleType) -> set[type]:
        return self.registry.register_module(target)
