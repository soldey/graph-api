from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.edges import EdgeTypeEnum, EdgeLevelEnum
from src.common.geometries import Geometry


class SelectEdgesDTO(BaseModel):
    graph: Optional[int] = Field(description="graph id", default=None)
    type: Optional[EdgeTypeEnum] = Field(description="type of edges", default=None)
    level: Optional[EdgeLevelEnum] = Field(description="level of edges", default=None)
    geometry: Optional[Geometry] = Field(description="area to look edges for", default=None)
