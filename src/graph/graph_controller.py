import json
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Body, Query, HTTPException
from loguru import logger
from starlette.responses import Response

from src.dependencies import graph_service
from src.edge.dto.create_edge_dto import CreateEdgeDTO, CreateEdgesDTO
from src.graph.dto.create_graph_dto import CreateGraphDTO
from src.graph.dto.graph_dto import GraphDTO
from src.graph.dto.graph_edge_dto import GraphEdgeDTO
from src.graph.dto.select_graph_with_edges_dto import SelectGraphWithEdgesDTO
from src.graph.dto.select_graphs_dto import SelectGraphsDTO

_router_prefix = "/graph"
graph_router = APIRouter(tags=["Graph controller"], prefix=_router_prefix)


@graph_router.post("/create", response_model=GraphDTO)
async def create(dto: Annotated[CreateGraphDTO, Body(embed=True)]):
    logger.info(f"Call - {_router_prefix}/create")
    
    return await GraphDTO.from_service(await graph_service.create(dto))


@graph_router.get("/select-one/{graph_id}", response_model=GraphDTO)
async def select_one(graph_id: Annotated[int, Path()]):
    logger.info(f"Call - {_router_prefix}/select-one/{graph_id}")
    
    return await GraphDTO.from_service(await graph_service.select_one(str(graph_id)))


@graph_router.get("/select-many", response_model=list[GraphDTO])
async def select_many(dto: Annotated[SelectGraphsDTO, Depends(SelectGraphsDTO)]):
    logger.info(f"Call - {_router_prefix}/select-many")
    
    return [await GraphDTO.from_service(graph) for graph in (await graph_service.select_many(dto))]


@graph_router.post("/add-edge", response_model=GraphEdgeDTO)
async def add_edge(dto: Annotated[CreateEdgeDTO, Body(embed=True)]):
    logger.info(f"Call - {_router_prefix}/add-edge")
    
    return await GraphEdgeDTO.from_service(await graph_service.add_edge(dto))


@graph_router.post("/add-edge-bulk", response_model=dict)
async def add_edge_bulk(
        dto: Annotated[CreateEdgesDTO, Body(embed=True)],
        graph: Annotated[int, Query()]
):
    logger.info(f"Call - {_router_prefix}/add-edge-bulk")
    
    return await graph_service.bulk_graph_upload(dto.nodes, dto.edges, graph)


@graph_router.post("/build", response_model=dict)
async def build_nx_graph(dto: Annotated[SelectGraphWithEdgesDTO, Depends(SelectGraphWithEdgesDTO)]):
    logger.info(f"Call - {_router_prefix}/build")
    
    try:
        graph_attrs, edges, nodes = await graph_service.build_nx_graph(dto)
    except Exception as e:
        raise HTTPException(500, str(e))
    return {
        "attributes": graph_attrs,
        "edges": json.loads(edges.to_json()),
        "nodes": json.loads(nodes.to_json())
    }


@graph_router.post("/visualize", response_class=Response)
async def visualize_graph(dto: Annotated[SelectGraphWithEdgesDTO, Depends(SelectGraphWithEdgesDTO)]):
    logger.info(f"Call - {_router_prefix}/visualize")
    
    png_bytes = await graph_service.visualize_graph(dto)
    return Response(content=png_bytes.read(), media_type="image/png")
