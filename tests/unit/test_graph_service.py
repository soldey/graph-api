from datetime import datetime
from enum import Enum

import pytest
import PIL.Image as Image
from fastapi import HTTPException

from src.graph.dto.select_graph_with_edges_dto import SelectGraphWithEdgesDTO
from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
from src.common.db.entities.graphs import GraphTypeEnum
from src.common.geometries import Geometry
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.graph.dto.create_graph_dto import CreateGraphDTO
from src.graph.dto.select_graphs_dto import SelectGraphsDTO
from src.graph.graph_entity import GraphEntity
from unit.test_edge_service import create_edge_dto, create_edge_dto2
from unit.test_node_service import create_node_dto, create_node_dto1

create_graph_dto = CreateGraphDTO(
    name="abc123",
    type=GraphTypeEnum.ROAD,
)
create_graph_dto2 = CreateGraphDTO(
    name="abc1234",
    type=GraphTypeEnum.ROAD,
)

area_geometry = Geometry(
    type="Polygon",
    coordinates=[
        [
            [
              30.30341613284787,
              59.9563529413482
            ],
            [
              30.306392781387444,
              59.9507485777969
            ],
            [
              30.305842305288365,
              59.950319789647466
            ],
            [
              30.32254008031694,
              59.952188040302076
            ],
            [
              30.321989604216327,
              59.95665916341429
            ],
            [
              30.30341613284787,
              59.9563529413482
            ]
        ]
    ]
)


@pytest.mark.asyncio
async def test_creating_graph(graph_service, create_graph):
    
    graph = create_graph
    expected = GraphEntity(
        0, create_graph_dto.name, create_graph_dto.type, create_graph_dto.properties, datetime.now(), datetime.now()
    )
    assert compare_entities(graph, expected)


@pytest.mark.asyncio
async def test_creating_graphs_with_same_name(graph_service, create_graph):

    _ = create_graph
    with pytest.raises(HTTPException) as e:
        await graph_service.create(create_graph_dto)
    assert e.value.status_code == 409


@pytest.mark.asyncio
async def test_creating_graph_with_numeric_name(graph_service):

    with pytest.raises(HTTPException) as e:
        await graph_service.create(CreateGraphDTO(
            name="123",
            type=GraphTypeEnum.ROAD
        ))
    assert e.value.status_code == 400


@pytest.mark.asyncio
async def test_selecting_graph_by_id_and_name(graph_service, create_graph):

    graph = create_graph
    assert compare_entities(await graph_service.select_one(str(graph.id)), graph, [])
    assert compare_entities(await graph_service.select_one(graph.name), graph, [])


@pytest.mark.asyncio
async def test_selecting_non_existing_graph(graph_service, create_graph):

    graph = create_graph
    with pytest.raises(HTTPException) as e:
        await graph_service.select_one(str(-graph.id))
    assert e.value.status_code == 404


@pytest.mark.asyncio
async def test_selecting_multiple_graphs(graph_service, create_graph, create_second_graph):

    g1 = create_graph
    g2 = create_second_graph
    assert len(await graph_service.select_many(SelectGraphsDTO(type=GraphTypeEnum.WATER))) == 0

    graphs = await graph_service.select_many(SelectGraphsDTO())
    for graph in graphs:
        assert compare_entities(graph, g1, []) or compare_entities(graph, g2, [])


@pytest.mark.asyncio
async def test_adding_edge_to_graph(graph_service, node_service, create_graph, create_nodes):
    from .test_edge_service import create_edge_dto
    
    graph = create_graph
    
    node1, node2 = create_nodes
    
    create_edge_dto.graph = graph.id
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    link = await graph_service.add_edge(create_edge_dto)
    
    assert link is not None
    assert link.graph == graph.id


@pytest.mark.asyncio
async def test_selecting_edges_and_nodes_by_graph(graph_service, node_service, create_graph, create_nodes):
    from .test_edge_service import create_edge_dto
    
    graph = create_graph
    
    node1, node2 = create_nodes
    
    create_edge_dto.graph = graph.id
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    _ = await graph_service.add_edge(create_edge_dto)
    graph1, edges, nodes = await graph_service.select_one_with_edges(
        SelectGraphWithEdgesDTO(id_or_name=str(graph.id))
    )
    
    assert compare_entities(graph, graph1)
    assert len(edges) == 1
    assert len(nodes) == 2


@pytest.mark.asyncio
async def test_selecting_edges_and_nodes_by_graph_when_multiple_present(
        graph_service, node_service, create_graph, create_second_graph, create_nodes
):
    from .test_edge_service import create_edge_dto
    from .test_node_service import create_node_dto, create_node_dto1
    
    g1 = create_graph
    g2 = create_second_graph
    
    node1, node2 = create_nodes
    
    create_edge_dto.graph = g1.id
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    create_edge_dto1 = CreateEdgeDTO(
        u=node2.id,
        v=node1.id,
        type=EdgeTypeEnum.DRIVE,
        weight=1,
        weight_type=WeightTypeEnum.DISTANCE,
        graph=g2.id,
        level=EdgeLevelEnum.REGIONAL,
        speed=60,
        geometry=Geometry(
            type="LineString",
            coordinates=[create_node_dto1.point.coordinates, create_node_dto.point.coordinates]
        )
    )
    
    _ = await graph_service.add_edge(create_edge_dto)
    _ = await graph_service.add_edge(create_edge_dto1)
    graph1, edges, nodes = await graph_service.select_one_with_edges(
        SelectGraphWithEdgesDTO(id_or_name=str(g1.id))
    )
    
    assert compare_entities(g1, graph1)
    assert len(edges) == 1
    assert len(nodes) == 2


@pytest.mark.asyncio
async def test_select_edges_and_nodes_by_non_existing_graph(graph_service, create_graph):
    graph = create_graph
    with pytest.raises(HTTPException) as e:
        _ = await graph_service.select_one_with_edges(
            SelectGraphWithEdgesDTO(id_or_name=str(-graph.id))
        )
    
    assert e.value.status_code == 404


@pytest.mark.asyncio
async def test_building_nx_graph(graph_service, create_graph, create_nodes):
    graph = create_graph
    node1, node2 = create_nodes
    
    from .test_edge_service import create_edge_dto
    
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    _ = await graph_service.add_edge(create_edge_dto)
    
    g = await graph_service.build_nx_graph(
        SelectGraphWithEdgesDTO(id_or_name=str(graph.id))
    )
    
    assert len(g.edges.items()) == 1


@pytest.mark.asyncio
async def test_building_nx_graph_with_multi_edge(graph_service, create_graph, create_nodes):
    graph = create_graph
    node1, node2 = create_nodes
    
    from .test_edge_service import create_edge_dto, create_edge_dto3
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    edge1 = await graph_service.add_edge(create_edge_dto)
    create_edge_dto3.u = node1.id
    create_edge_dto3.v = node2.id
    create_edge_dto3.graph = graph.id
    edge2 = await graph_service.add_edge(create_edge_dto3)
    assert edge1.id != edge2.id
    
    g = await graph_service.build_nx_graph(
        SelectGraphWithEdgesDTO(id_or_name=str(graph.id))
    )
    
    assert len(g.edges.items()) == 2
    
    # png_graph_bytes = await graph_service.visualize_graph(str(graph.id))
    # image = Image.open(png_graph_bytes)
    # image.show()


@pytest.mark.asyncio
async def test_getting_graph_with_edges_by_geometry(graph_service, create_graph, create_edges, create_nodes):
    from .test_edge_service import create_edge_dto
    
    graph = create_graph
    
    node1, node2 = create_nodes
    
    create_edge_dto.graph = graph.id
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    _ = await graph_service.add_edge(create_edge_dto)
    dto = SelectGraphWithEdgesDTO(geometry=area_geometry)
    graph1, edges, nodes = await graph_service.select_one_with_edges(
        dto
    )
    assert graph1 is None
    assert len(edges) == 2
    assert len(nodes) == 2


@pytest.mark.asyncio
async def test_building_nx_graph_by_geometry(graph_service, create_graph, create_nodes):
    graph = create_graph
    node1, node2 = create_nodes
    
    from .test_edge_service import create_edge_dto, create_edge_dto3
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    edge1 = await graph_service.add_edge(create_edge_dto)
    create_edge_dto3.u = node1.id
    create_edge_dto3.v = node2.id
    create_edge_dto3.graph = graph.id
    edge2 = await graph_service.add_edge(create_edge_dto3)
    assert edge1.id != edge2.id
    
    g = await graph_service.build_nx_graph(
        SelectGraphWithEdgesDTO(geometry=area_geometry)
    )
    
    assert len(g.edges.items()) == 2
    assert len(g.nodes.items()) == 2


@pytest.mark.asyncio
async def test_bulk_graph_upload(graph_service, create_graph):
    nodes = [create_node_dto, create_node_dto1]
    edges = [create_edge_dto, create_edge_dto2]
    
    graph = create_graph
    
    await graph_service.bulk_graph_upload(nodes, edges, graph.id)


def compare_entities(graph1: GraphEntity, graph2: GraphEntity, ignore: list[str] = ["id", "created_at", "updated_at"]):
    graph1 = graph1.__dict__
    graph2 = graph2.__dict__
    for k, v in graph1.items():
        if k not in ignore and (k not in graph2 or v != graph2[k]):
            return False
    return True
