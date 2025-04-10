from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.nodes import NodeTypeEnum
from src.common.geometries import Geometry


class CreateNodeDTO(BaseModel):
    type: NodeTypeEnum = Field(examples=[NodeTypeEnum.CROSSROAD, NodeTypeEnum.STOP], default=NodeTypeEnum.CROSSROAD)
    properties: dict = Field(default={}, description="optional, additional properties")
    route: Optional[str] = Field(default=None, description="optional, route name")
    point: Geometry

