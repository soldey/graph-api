from dataclasses import dataclass


@dataclass()
class GraphEdgeEntity:
    id: int
    graph: int
    edge: int
