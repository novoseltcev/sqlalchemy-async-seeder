from pydantic import BaseModel


class SeedReference(BaseModel):
    target_class: str
    criteria: dict[str, str]
    field: str | None = None


class SeedData(BaseModel):
    refs: dict[str, SeedReference] | None = None


class SeedGroup(BaseModel):
    target_class: str
    data: SeedData | list[SeedData]

    class Config:
        extra = "ignore"
