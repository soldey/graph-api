from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from loguru import logger

from src.admin_controller import admin_router
from src.edge.edge_controller import edge_router
from src.graph.graph_controller import graph_router
from src.node.node_controller import node_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


async def logging_dependency(request: Request):
    logger.info(f"{request.method} {request.url}")


app = FastAPI(
    title="Graph API",
    root_path="/api/v1",
    version="1.6.0",
    lifespan=lifespan
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin_router, dependencies=[Depends(logging_dependency)])
app.include_router(graph_router, dependencies=[Depends(logging_dependency)])
app.include_router(edge_router, dependencies=[Depends(logging_dependency)])
app.include_router(node_router, dependencies=[Depends(logging_dependency)])
