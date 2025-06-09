from typing import Annotated

from fastapi import APIRouter, Depends, Path, Body
from loguru import logger

from src.dependencies import edge_service
from src.edge.dto.create_edge_dto import CreateEdgeDTO, CreateEdgesDTO
from src.edge.dto.edge_dto import EdgeDTO
from src.edge.dto.select_edges_dto import SelectEdgesDTO

_router_prefix = "/edge"
edge_router = APIRouter(tags=["Edge Controller"], prefix=_router_prefix)


@edge_router.post("/create", response_model=EdgeDTO)
async def create_edge(dto: Annotated[CreateEdgeDTO, Body(embed=True)]) -> EdgeDTO:
    logger.info(f"Call - {_router_prefix}/create")
    
    return await EdgeDTO.from_service(await edge_service.create(dto))


@edge_router.post("/create-bulk", response_model=list[EdgeDTO])
async def create_edge_bulk(dto: Annotated[CreateEdgesDTO, Body()]) -> list[EdgeDTO]:
    logger.info(f"Call - {_router_prefix}/create-bulk")
    result, uploaded = await edge_service.create_many(dto.dtos)
    return [await EdgeDTO.from_service(edge) for edge in (result)]


@edge_router.get("/select-one/{edge_id}", response_model=EdgeDTO)
async def select_one(edge_id: Annotated[int, Path()]) -> EdgeDTO:
    logger.info(f"Call - {_router_prefix}/select-one/{edge_id}")
    
    return await EdgeDTO.from_service(await edge_service.select_one(edge_id))


@edge_router.post("/select-many", response_model=list[EdgeDTO])
async def select_many(dto: Annotated[SelectEdgesDTO, Depends(SelectEdgesDTO)]) -> list[EdgeDTO]:
    logger.info(f"Call - {_router_prefix}/select-many")
    
    return [await EdgeDTO.from_service(edge) for edge in (await edge_service.select_many(dto))]


@edge_router.delete("/delete/{edge_id}")
async def delete_edge(edge_id: Annotated[int, Path()]):
    logger.info(f"Call - {_router_prefix}/delete/{edge_id}")
    
    return await edge_service.delete_edge(edge_id)
