from pydantic import BaseModel, Field

from src.common.db.entities.graphs import GraphTypeEnum


class CreateGraphDTO(BaseModel):
    name: str = Field(description="name of graph")
    type: GraphTypeEnum = Field(examples=[GraphTypeEnum.ROAD, GraphTypeEnum.WATER], description="type of graph")
    properties: dict = Field(default={}, description="optional, additional properties")
