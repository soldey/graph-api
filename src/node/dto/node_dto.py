from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.nodes import NodeTypeEnum
from src.common.geometries import Geometry
from src.node.node_entity import NodeEntity


class NodeDTO(BaseModel):
    id: int = Field(ge=0)
    type: NodeTypeEnum = Field(description="node type", examples=[NodeTypeEnum.CROSSROAD, NodeTypeEnum.STOP])
    point: Geometry = Field(description="geometry of node")
    route: Optional[str] = Field(description="route name", default=None)
    properties: dict = Field(description="additional parameters")
    created_at: datetime = Field(description="date of creation", default=datetime.now())
    updated_at: datetime = Field(description="date of update", default=datetime.now())
    
    @classmethod
    async def from_service(cls, entity: NodeEntity):
        return cls(
            id=entity.id,
            type=entity.type,
            route=entity.route,
            point=Geometry.from_shapely_geometry(entity.point),
            created_at=entity.created_at,
            updated_at=entity.updated_at,
        )
