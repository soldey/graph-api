from typing import Optional

from pydantic import BaseModel, Field

from src.node.dto.create_node_dto import CreateNodeDTO
from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
from src.common.geometries import Geometry


class CreateEdgeDTO(BaseModel):
    u: int | CreateNodeDTO = Field(description="source node id")
    v: int | CreateNodeDTO = Field(description="dest node id")
    type: EdgeTypeEnum = Field(examples=[EdgeTypeEnum.DRIVE, EdgeTypeEnum.WATERCHANNEL], description="type of edge")
    weight: float = Field(description="value of edge weight")
    weight_type: WeightTypeEnum = Field(examples=[WeightTypeEnum.DISTANCE, WeightTypeEnum.TIME], description="type of weight")
    graph: int = Field(description="graph id")
    level: EdgeLevelEnum = Field(examples=[EdgeLevelEnum.NONE, EdgeLevelEnum.REGIONAL], description="road level of edge")
    speed: int = Field(description="max speed of edge")
    route: Optional[str] = Field(description="route name", default=None)
    properties: dict = Field(default={}, description="optional, additional properties")
    geometry: Geometry | None = Field(default=None)


class CreateEdgesDTO(BaseModel):
    dtos: list[CreateEdgeDTO] = Field(description="list of dtos")
