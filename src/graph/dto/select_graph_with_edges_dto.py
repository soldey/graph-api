from typing import Optional

from pydantic import BaseModel, Field

from src.common.geometries import Geometry


class SelectGraphWithEdgesDTO(BaseModel):
    id_or_name: Optional[str] = Field(description="graph id or name", default=None)
    geometry: Optional[Geometry] = Field(description="area to look edges and nodes for", default=None)
