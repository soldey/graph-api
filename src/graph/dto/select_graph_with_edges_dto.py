from typing import Optional, Literal

from pydantic import BaseModel, Field

from src.common.geometries import Geometry


class SelectGraphWithEdgesDTO(BaseModel):
    id_or_name: Optional[str] = Field(description="graph id or name", default=None)
    geometry: Optional[Geometry] = Field(description="area to look edges and nodes for", default=None)
    type: Literal["walk", "drive", "intermodal", "water"] = Field(description="type of graph to retrieve", default="intermodal")
