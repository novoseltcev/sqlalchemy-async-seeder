class UnresolvedReferencesError(Exception):
    """Raised when a reference could not be resolved during the seeding process."""


class AmbiguousReferenceError(Exception):
    """Raised when a reference matches more than one entity."""


class EntityBuildError(Exception):
    """Internal error to signify that an entity cannot be built."""
