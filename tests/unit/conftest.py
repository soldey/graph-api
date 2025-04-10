import asyncio

import pytest_asyncio
from alembic import command
from fastapi import HTTPException
from iduconfig import Config
from alembic.config import Config as AlembicConfig

from src.common.db.database import DatabaseModule
from src.edge.edge_service import EdgeService
from src.graph.graph_service import GraphService
from src.node.node_service import NodeService
from tests.unit.test_edge_service import create_edge_dto, create_graph_dto, \
    create_graph_dto2, create_edge_dto3
from unit.test_node_service import create_node_dto, create_node_dto1


@pytest_asyncio.fixture(scope="session")
async def database():
    config = Config()
    database = DatabaseModule(config)
    
    alembic_config = AlembicConfig('alembic.ini')
    await asyncio.to_thread(command.downgrade, alembic_config, "base")
    await asyncio.to_thread(command.upgrade, alembic_config, "head")
    yield database
    #
    # try:
    #     yield database
    # finally:
    #     await asyncio.to_thread(command.downgrade, alembic_config, "base")


@pytest_asyncio.fixture(scope="session")
async def graph_service(database):
    config = Config()
    node_service = NodeService(config, database)
    edge_service = EdgeService(config, database, node_service)
    graph_service = GraphService(database, node_service, edge_service)
    yield graph_service


@pytest_asyncio.fixture(scope="session")
async def node_service(database):
    config = Config()
    node_service = NodeService(config, database)
    yield node_service


@pytest_asyncio.fixture(scope="session")
async def services(database):
    config = Config()
    node_service = NodeService(config, database)
    edge_service = EdgeService(config, database, node_service)
    graph_service = GraphService(database, node_service, edge_service)
    yield node_service, edge_service, graph_service


@pytest_asyncio.fixture(scope="session")
async def create_nodes(node_service):
    try:
        node1 = await node_service.create(create_node_dto)
    except HTTPException as e:
        if e.status_code == 409:
            result, session = await node_service.select_one_by_geometry(create_node_dto.point.as_shapely_geometry())
            await session.close()
            node1 = await node_service.select_one(result)
        else:
            raise e
    try:
        node2 = await node_service.create(create_node_dto1)
    except HTTPException as e:
        if e.status_code == 409:
            result, session = await node_service.select_one_by_geometry(create_node_dto1.point.as_shapely_geometry())
            await session.close()
            node2 = await node_service.select_one(result)
        else:
            raise e
    yield node1, node2


@pytest_asyncio.fixture(scope="session")
async def create_edges(services, create_nodes):
    _, edge_service, _ = services
    node1, node2 = create_nodes
    create_edge_dto.u = node1.id
    create_edge_dto.v = node2.id
    
    edge = await edge_service.create(create_edge_dto)
    yield edge


@pytest_asyncio.fixture(scope="session")
async def create_multiedge(services, create_nodes):
    _, edge_service, _ = services
    node1, node2 = create_nodes
    create_edge_dto3.u = node1.id
    create_edge_dto3.v = node2.id
    
    edge = await edge_service.create(create_edge_dto3)
    yield edge


@pytest_asyncio.fixture(scope="session")
async def create_graph(graph_service):
    try:
        graph = await graph_service.create(create_graph_dto)
    except HTTPException as e:
        if e.status_code == 409:
            graph = await graph_service.select_one(create_graph_dto.name)
        else:
            raise e
    yield graph


@pytest_asyncio.fixture(scope="session")
async def create_second_graph(graph_service):
    try:
        graph = await graph_service.create(create_graph_dto2)
    except HTTPException as e:
        if e.status_code == 409:
            graph = await graph_service.select_one(create_graph_dto2.name)
        else:
            raise e
    yield graph
