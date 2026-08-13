from pydantic import BaseModel


class Feature(BaseModel):
    """Associate an output column with a SACZ polygon identifier."""

    area: str
    shape: str


class FeatureCollection(BaseModel):
    """Describe one atmospheric field and its SACZ extraction regions."""

    variable: str
    nc_name: str
    out_name: str | None
    level: int
    source: str
    features: list[Feature]
