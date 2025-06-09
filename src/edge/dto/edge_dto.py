from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
from src.common.geometries import Geometry
from src.edge.edge_entity import EdgeEntity


class EdgeDTO(BaseModel):
    id: int = Field(ge=0)
    u: int = Field(description="source node id", ge=0)
    v: int = Field(description="destination node id", ge=0)
    type: EdgeTypeEnum = Field(description="type of edge", examples=[EdgeTypeEnum.DRIVE, EdgeTypeEnum.WATERCHANNEL])
    weight: float = Field(description="weight of edge")
    weight_type: WeightTypeEnum = Field(
        description="weight type", examples=[WeightTypeEnum.DISTANCE, WeightTypeEnum.TIME]
    )
    level: EdgeLevelEnum = Field(description="level of edge", examples=[EdgeLevelEnum.REGIONAL, EdgeLevelEnum.LOCAL])
    speed: int = Field(description="max speed of edge")
    route: Optional[str] = Field(description="route name", default=None)
    properties: dict = Field(description="additional parameters", default={})
    geometry: Geometry = Field(description="geometry of edge")
    created_at: datetime = Field(description="date of creation", default=datetime.now())
    updated_at: datetime = Field(description="date of last update", default=datetime.now())
    
    @classmethod
    async def from_service(cls, entity: EdgeEntity):
        return cls(
            id=entity.id,
            u=entity.u,
            v=entity.v,
            type=entity.type,
            weight=entity.weight,
            weight_type=entity.weight_type,
            level=entity.level,
            speed=entity.speed,
            route=entity.route,
            geometry=Geometry.from_shapely_geometry(entity.geometry),
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
