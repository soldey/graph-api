from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.nodes import NodeTypeEnum
from src.common.geometries import Geometry


class SelectNodesDTO(BaseModel):
    graph: Optional[int] = Field(description="graph id", default=None)
    type: Optional[NodeTypeEnum | list[NodeTypeEnum]] = Field(description="type of node", default=None)
    geometry: Optional[Geometry] = Field(description="area to look nodes for", default=None)
