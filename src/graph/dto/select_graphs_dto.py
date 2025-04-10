from typing import Optional

from pydantic import BaseModel, Field

from src.common.db.entities.graphs import GraphTypeEnum


class SelectGraphsDTO(BaseModel):
    type: Optional[GraphTypeEnum] = Field(description="graph type", default=None)
