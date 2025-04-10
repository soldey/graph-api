from datetime import datetime

import pytest
from fastapi import HTTPException
import shapely.geometry as geom
from pandas import DataFrame
from shapely import equals_exact

from src.common.db.entities.nodes import NodeTypeEnum
from src.common.geometries import Geometry
from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.dto.select_nodes_dto import SelectNodesDTO
from src.node.node_entity import NodeEntity


create_node_dto = CreateNodeDTO(
    type=NodeTypeEnum.DRIVE,
    point=Geometry(
        type='Point',
        coordinates=[30.307160601007837, 59.94951849764507]
    ),
    route="route 4"
)
create_node_dto1 = CreateNodeDTO(
    type=NodeTypeEnum.DRIVE,
    point=Geometry(
        type='Point',
        coordinates=[30.32309539596909, 59.95291638534613]
    ),
    route="route 5"
)


@pytest.mark.asyncio
async def test_creating_node(node_service, create_nodes):
    
    node, _ = create_nodes
    expected = NodeEntity(
        0, NodeTypeEnum.DRIVE,
        geom.shape({"type": create_node_dto.point.type, "coordinates": create_node_dto.point.coordinates}),
        {},
        datetime.now(), datetime.now()
    )
    assert compare_entities(node, expected)


@pytest.mark.asyncio
async def test_select_node(node_service, create_nodes):

    node, _ = create_nodes
    assert compare_entities(node, await node_service.select_one(node.id))


@pytest.mark.asyncio
async def test_selecting_non_existing_node(node_service, create_nodes):

    node, _ = create_nodes
    with pytest.raises(HTTPException) as e:
        await node_service.select_one(-node.id)
    assert e.value.status_code == 404


@pytest.mark.asyncio
async def test_select_nodes(node_service, create_nodes):

    node1, node2 = create_nodes

    nodes = await node_service.select_many(SelectNodesDTO(type=NodeTypeEnum.PLATFORM))
    assert len(nodes) == 0

    nodes = await node_service.select_many(SelectNodesDTO())
    assert len(nodes) == 2
    for node in nodes:
        assert compare_entities(node, node1, ignore=[]) or compare_entities(node, node2, ignore=[])


@pytest.mark.asyncio
async def test_creating_nodes_with_equal_geometry(node_service, create_nodes):
    node, _ = create_nodes
    node1 = await node_service.create(create_node_dto)
    assert compare_entities(node, node1)


@pytest.mark.asyncio
async def test_test(node_service):
    data = [create_node_dto, create_node_dto1]
    cnt = 4
    for dto in data:
        dto.route = f"route {cnt}"
        cnt += 2
    df_nodes = DataFrame(data=[dto.__dict__ for dto in data])
    res = await node_service.create_many(df_nodes)
    print(res)
    assert len(res) == 2


def compare_entities(node1: NodeEntity, node2: NodeEntity, ignore: list[str] = ["id", "created_at", "updated_at"]):
    node1 = node1.__dict__
    node2 = node2.__dict__
    for k, v in node1.items():
        if k == "point" and not equals_exact(v, node2[k], 0.2):
            return False
        elif k not in ignore + ["point"] and (k not in node2 or v != node2[k]):
            return False
    return True
