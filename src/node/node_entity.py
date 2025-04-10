from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import shapely.geometry as geom

from src.common.db.entities.nodes import NodeTypeEnum


@dataclass()
class NodeEntity:
    id: int
    type: "NodeTypeEnum"
    point: geom.Point
    route: Optional[str]
    properties: dict
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if isinstance(self.point, dict):
            self.point = geom.shape(self.point)
    
    def __hash__(self):
        return (self.id + 1) * self.point.__hash__()
