from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from .builder import EntityBuilder
from .errors import UnresolvedReferencesError
from .registry import ClassRegistry


@dataclass
class Seed:
    target_class: str
    data: list[dict[str, Any]] | dict[str, Any]


class ReferenceResolver:
    def __init__(
        self,
        session: AsyncSession,
        registry: ClassRegistry,
        flush_on_create: bool = False,
    ) -> None:
        self.session = session
        self.registry = registry
        self.flush_on_create = flush_on_create

    async def generate_entities(self, seed: Seed | list[Seed]) -> list[object]:
        if isinstance(seed, list):
            entity_builders = []
            for group_data in seed:
                group_builders = self._generate_builders_from_group(group_data)
                entity_builders.extend(group_builders)
            return await self._resolve_builders(entity_builders)

        return await self._resolve_builders(self._generate_builders_from_group(seed))

    def _generate_builders_from_group(
        self, entity_group_dict: Seed
    ) -> list[EntityBuilder]:
        target_cls = self.registry[entity_group_dict.target_class]
        target_data = entity_group_dict.data
        if isinstance(target_data, list):
            return [
                self._generate_builder_from_data_block(target_cls, data_block)
                for data_block in target_data
            ]
        return [self._generate_builder_from_data_block(target_cls, target_data)]

    def _generate_builder_from_data_block(
        self, target_cls: type, data_dict: dict
    ) -> EntityBuilder:
        return EntityBuilder(
            session=self.session,
            registry=self.registry,
            target_cls=target_cls,
            data_block=data_dict,
        )

    async def _resolve_builders(
        self, entity_builders: list[EntityBuilder]
    ) -> list[object]:
        entities = []
        previous_builder_count = len(entity_builders)
        while len(entity_builders) > 0:
            for builder in entity_builders:
                if await builder.resolve():
                    entity = await self._resolve_builder(builder)
                    entities.append(entity)
                    entity_builders.remove(builder)

            if previous_builder_count == len(entity_builders):  # No progress being made
                raise UnresolvedReferencesError(
                    f"'{len(entity_builders)}' builders have unresolvable references."
                )
            previous_builder_count = len(entity_builders)
        return entities

    async def _resolve_builder(self, builder: EntityBuilder) -> object:
        entity = builder.build()
        self.session.add(entity)
        if self.flush_on_create:
            await self.session.flush()
        return entity
