from datetime import datetime

import pytest
import shapely.geometry as geom
from shapely import equals_exact

from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
from src.common.db.entities.graphs import GraphTypeEnum
from src.common.geometries import Geometry
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.edge.edge_entity import EdgeEntity
from src.graph.dto.create_graph_dto import CreateGraphDTO
from unit.test_node_service import create_node_dto, create_node_dto1

create_graph_dto = CreateGraphDTO(
    name="abc123",
    type=GraphTypeEnum.ROAD,
)
create_graph_dto2 = CreateGraphDTO(
    name="abcd1234",
    type=GraphTypeEnum.ROAD
)
create_edge_dto = CreateEdgeDTO(
    u=0,
    v=1,
    type=EdgeTypeEnum.DRIVE,
    weight=1.,
    weight_type=WeightTypeEnum.DISTANCE,
    graph=1,
    level=EdgeLevelEnum.REGIONAL,
    speed=60,
    geometry=Geometry(
        type="LineString",
        coordinates=[create_node_dto.point.coordinates, create_node_dto1.point.coordinates]
    )
)
create_edge_dto2 = CreateEdgeDTO(
    u=0,
    v=1,
    type=EdgeTypeEnum.DRIVE,
    weight=1.,
    weight_type=WeightTypeEnum.DISTANCE,
    graph=2,
    level=EdgeLevelEnum.REGIONAL,
    speed=60,
    geometry=Geometry(
        type="LineString",
        coordinates=[create_node_dto1.point.coordinates, create_node_dto.point.coordinates]
    )
)
coords = [30.311498129815618, 59.956705096992096]
create_edge_dto3 = CreateEdgeDTO(
    u=0,
    v=1,
    type=EdgeTypeEnum.DRIVE,
    weight=1.,
    weight_type=WeightTypeEnum.DISTANCE,
    graph=1,
    level=EdgeLevelEnum.REGIONAL,
    speed=60,
    geometry=Geometry(
        type="MultiLineString",
        coordinates=[
            [create_node_dto.point.coordinates, coords],
            [coords, create_node_dto1.point.coordinates]
]
    )
)


@pytest.mark.asyncio
async def test_creating_edge(services, create_nodes):
    node_service, edge_service, graph_service = services

    node1, node2 = create_nodes
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id

    expected = EdgeEntity(
        0,
        node1.id,
        node2.id,
        create_edge_dto.type,
        create_edge_dto.weight,
        create_edge_dto.weight_type,
        create_edge_dto.level,
        create_edge_dto.speed,
        create_edge_dto.properties,
        geom.shape({"type": create_edge_dto.geometry.type, "coordinates": create_edge_dto.geometry.coordinates}),
        datetime.now(),
        datetime.now()
    )
    assert compare_entities(await edge_service.create(create_edge_dto), expected)


@pytest.mark.asyncio
async def test_select_one_edge(services, create_nodes):
    node_service, edge_service, graph_service = services
    
    node1, node2 = create_nodes
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    edge = await edge_service.create(create_edge_dto)
    assert compare_entities(await edge_service.select_one(edge.id), edge)


@pytest.mark.asyncio
async def test_creating_edges_with_equal_geometry(services, create_nodes, create_edges):
    node_service, edge_service, graph_service = services

    edge1 = create_edges
    
    create_edge_dto.weight = edge1.weight + 1
    edge2 = await edge_service.create(create_edge_dto)
    assert compare_entities(edge1, edge2)


@pytest.mark.asyncio
async def test_creating_edge_with_node_dto(services, create_nodes, create_edges):
    _, edge_service, _ = services
    
    edge1 = create_edges
    
    create_edge_dto.u = create_node_dto
    create_edge_dto.v = create_node_dto1
    edge2 = await edge_service.create(create_edge_dto)
    assert compare_entities(edge1, edge2)


def compare_entities(edge1: EdgeEntity, edge2: EdgeEntity):
    edge1 = edge1.__dict__
    edge2 = edge2.__dict__
    for k, v in edge1.items():
        if k == "geometry" and not equals_exact(v, edge2[k], 0.2):
            return False
        elif k not in ["id", "created_at", "updated_at", "geometry"] and (k not in edge2 or v != edge2[k]):
            return False
    return True
