from typing import Any, NamedTuple

from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession

from .errors import AmbiguousReferenceError, EntityBuildError, UnresolvedReferencesError
from .registry import ClassRegistry


class EntityReference(NamedTuple):
    cls: type
    field: str
    src_field: str
    criteria: dict[str, str]


class EntityBuilder:
    """
    A builder corresponds to one entity block and thus can only ever build once.
    Multiple attempts to build will throw a EntityBuildError.
    """

    def __init__(
        self,
        session: AsyncSession,
        registry: ClassRegistry,
        target_cls: type,
        data_block: dict,
    ) -> None:
        self.session = session
        self.registry = registry
        self.target_cls: type = target_cls
        self.references = self._init_refs(data_block.pop("refs", {}))
        self.data = data_block
        self.built = False

    def _init_refs(self, refs_block: dict[str, Any]) -> list[EntityReference]:
        return [
            EntityReference(
                cls=self.registry[reference["target_class"]],
                field=reference["field"] if "field" in reference else "",
                src_field=field,
                criteria=reference["criteria"],
            )
            for field, reference in refs_block.items()
        ]

    @property
    def resolved(self) -> bool:
        return len(self.references) == 0

    def build(self) -> object:
        if not self.resolved:
            raise UnresolvedReferencesError("Entity Builder has unresolved references.")

        if self.built:
            raise EntityBuildError("Entity Builder has already been used.")

        self.built = True
        return self.target_cls(**self.data)

    async def resolve(self) -> bool:
        """Return True if fully resolved, False otherwise."""
        if self.resolved:
            return True

        for reference in self.references:
            try:
                cursor = await self.session.execute(
                    select(reference.cls).filter_by(**reference.criteria)
                )
                reference_entity = cursor.scalar()
            except MultipleResultsFound:
                raise AmbiguousReferenceError(
                    "Matched more than one entity of class '{}'".format(reference.cls)
                )
            if reference_entity:
                self.data[reference.src_field] = (
                    getattr(reference_entity, reference.field)
                    if reference.field
                    else reference_entity
                )
                self.references.remove(reference)

        return self.resolved
