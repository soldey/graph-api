from datetime import datetime

from pydantic import BaseModel, Field

from src.common.db.entities.graphs import GraphTypeEnum
from src.graph.graph_entity import GraphEntity


class GraphDTO(BaseModel):
    id: int = Field(ge=0)
    name: str = Field(description="name of graph")
    type: GraphTypeEnum = Field(description="type of graph", examples=[GraphTypeEnum.ROAD])
    properties: dict = Field(description="additional parameters")
    created_at: datetime = Field(description="date of creation")
    updated_at: datetime = Field(description="date of last update")
    
    @classmethod
    async def from_service(cls, entity: GraphEntity):
        return cls(**entity.__dict__)
