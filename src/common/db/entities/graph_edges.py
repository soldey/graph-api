from typing import Callable

from sqlalchemy import Sequence, Table, Column, Integer, ForeignKey

from src.common.db import metadata
from src.common.db.entities.edges import edges
from src.common.db.entities.graphs import graphs

func: Callable

graph_edges_id_seq = Sequence("graph_edges_id_seq")
graph_edges = Table(
    "graph_edges",
    metadata,
    Column("id", Integer, primary_key=True, server_default=graph_edges_id_seq.next_value()),
    Column("graph", Integer, ForeignKey(graphs.c.id, ondelete="CASCADE"), nullable=False),
    Column("edge", Integer, ForeignKey(edges.c.id, ondelete="CASCADE"), nullable=False),
)
