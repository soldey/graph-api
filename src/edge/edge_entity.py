from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
import shapely.geometry as geom


@dataclass()
class EdgeEntity:
    id: int
    u: int
    v: int
    type: "EdgeTypeEnum"
    weight: float
    weight_type: "WeightTypeEnum"
    level: "EdgeLevelEnum"
    speed: int
    route: Optional[str]
    properties: dict
    geometry: geom.LineString | geom.Polygon | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.geometry, dict):
            self.geometry = geom.shape(self.geometry) if self.geometry else None
