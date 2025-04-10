from dataclasses import dataclass
from datetime import datetime

from src.common.db.entities.graphs import GraphTypeEnum


@dataclass()
class GraphEntity:
    id: int
    name: str
    type: "GraphTypeEnum"
    properties: dict
    created_at: datetime
    updated_at: datetime
