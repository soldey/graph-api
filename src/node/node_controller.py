from typing import Annotated

from fastapi import APIRouter, Depends, Path, Body
from loguru import logger

from src.dependencies import node_service
from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.dto.node_dto import NodeDTO
from src.node.dto.select_nodes_dto import SelectNodesDTO

_router_prefix = "/node"
node_router = APIRouter(tags=["Node Controller"], prefix=_router_prefix)


@node_router.post("/create", response_model=NodeDTO)
async def create(dto: Annotated[CreateNodeDTO, Body(embed=True)]) -> NodeDTO:
    logger.info(f"Call - {_router_prefix}/create")
    
    return await NodeDTO.from_service(await node_service.create(dto))


@node_router.get("/select-one/{node_id}", response_model=NodeDTO)
async def select_one(node_id: Annotated[int, Path()]) -> NodeDTO:
    logger.info(f"Call - {_router_prefix}/select-one/{node_id}")
    
    return await NodeDTO.from_service(await node_service.select_one(node_id))


@node_router.post("/select-many", response_model=list[NodeDTO])
async def select_many(dto: Annotated[SelectNodesDTO, Depends(SelectNodesDTO)]) -> list[NodeDTO]:
    logger.info(f"Call - {_router_prefix}/select-many")
    
    return [await NodeDTO.from_service(node) for node in (await node_service.select_many(dto))]


@node_router.delete("/delete/{node_id}")
async def delete_node(node_id: Annotated[int, Path()]):
    logger.info(f"Call - {_router_prefix}/delete/{node_id}")
    
    return await node_service.delete_node(node_id)
