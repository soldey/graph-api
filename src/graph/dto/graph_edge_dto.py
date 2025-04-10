from pydantic import BaseModel

from src.graph.graph_edge_entity import GraphEdgeEntity


class GraphEdgeDTO(BaseModel):
    id: int
    graph: int
    edge: int
    
    @classmethod
    async def from_service(cls, entity: GraphEdgeEntity):
        return cls(**entity.__dict__)
