import concurrent.futures
import csv
import hashlib
from io import StringIO, BytesIO
from json.decoder import NaN
from math import ceil
from pathlib import Path

from asyncpg import UniqueViolationError
from fastapi import HTTPException
from iduconfig import Config
from loguru import logger
from pandas import DataFrame
from shapely import from_wkb
from sqlalchemy import insert, cast, text, select, delete
from geoalchemy2 import functions as geofunc
from sqlalchemy.dialects.postgresql import JSONB
import shapely.geometry as geom
from sqlalchemy.ext.asyncio import AsyncSession

from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.node_service import NodeService
from src.common.db.database import DatabaseModule
from src.common.db.entities.edges import edges
from src.common.db.entities.graph_edges import graph_edges
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.edge.dto.select_edges_dto import SelectEdgesDTO
from src.edge.edge_entity import EdgeEntity


class EdgeService:
    def __init__(self, config: Config, database: DatabaseModule, node_service: NodeService):
        self.database = database
        self.node_service = node_service
        self.csv_dir = Path().absolute() / config.get("CSV_DIR")
        self.csv_dir.mkdir(exist_ok=True)

    async def create(self, dto: CreateEdgeDTO) -> EdgeEntity:
        """Creates one edge or returns existing one by geometry.
        
        Args:
            dto (CreateEdgeDTO): DTO object.
        Returns:
            EdgeEntity: edge object.
        """
        
        logger.info("Started creating edge")
        edge, session = await self.select_one_by_geometry(dto.geometry.as_shapely_geometry())
        if edge is not None:
            await session.close()
            logger.info("Found existing edge")
            return await self.select_one(edge)
        
        if isinstance(dto.u, CreateNodeDTO):
            node_u = await self.node_service.create(dto.u)
        if isinstance(dto.v, CreateNodeDTO):
            node_v = await self.node_service.create(dto.v)
        
        statement = (
            insert(edges).values(
                u=dto.u if isinstance(dto.u, int) else node_u.id,
                v=dto.v if isinstance(dto.v, int) else node_v.id,
                type=dto.type,
                weight=dto.weight,
                weight_type=dto.weight_type,
                level=dto.level,
                speed=dto.speed,
                properties=dto.properties,
                geometry=geofunc.ST_GeomFromText(str(dto.geometry.as_shapely_geometry()), text("4326"))
            )
        ).returning(
            edges.c.id,
            edges.c.u,
            edges.c.v,
            edges.c.type,
            edges.c.weight,
            edges.c.weight_type,
            edges.c.level,
            edges.c.speed,
            edges.c.properties,
            cast(geofunc.ST_AsGeoJSON(edges.c.geometry), JSONB).label("geometry"),
            edges.c.created_at,
            edges.c.updated_at
        )

        result, session = await self.database.execute_with_session(statement, session)
        result = result.mappings().one()

        await session.commit()
        await session.close()
        
        logger.info("Edge committed")
        return EdgeEntity(**result)
    
    async def create_many(self, df_edges: DataFrame) -> DataFrame:
        """Bulk edge creation.
        
        Args:
            df_edges (DataFrame):

        Returns:

        """
        
        logger.info("Starting bulk edge upload")
        
        df_edges.drop("graph", axis="columns", inplace=True)
        df_edges["geometry"] = df_edges["geometry"].apply(lambda x: "SRID=4326; " + str(x.as_shapely_geometry()))
        df_edges["type"] = df_edges["type"].apply(lambda x: x.value)
        df_edges["weight_type"] = df_edges["weight_type"].apply(lambda x: x.value)
        df_edges["level"] = df_edges["level"].apply(lambda x: x.value)
        df_edges["route"] = df_edges["route"].fillna("")
        df = df_edges.copy()
        
        df_hash = hashlib.sha256(df['geometry'].values).hexdigest()
        csv_name = f"edges_{df_hash[:12]}.csv"
        
        df_edges["new_id"] = [None] * len(df_edges)
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(self.csv_dir / csv_name, sep="&", header=False, lineterminator="\n", index=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
            try:
                res = [record["id"] for record in await self.database.execute_copy("edges", str(self.csv_dir / csv_name), columns)]
                logger.success(f"Successfully added {len(res)} edges")

                j = 0
                for i in range(len(df_edges)):
                    if df_edges["new_id"][i] is None:
                        df_edges.loc[i, "new_id"] = res[j]
                        j += 1
                
                done = True
            except UniqueViolationError as e:
                logger.error(e)
                df_edges, df = await self._conflict_resolver(e, df_edges, df)
        
        (self.csv_dir / csv_name).unlink()
        return df_edges
    
    async def _conflict_resolver(self, e: Exception, df_edges: DataFrame, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        TODO: conflict resolver for existing nodes, more research towards returned geometry type (hash?)
        """
        
        filtered_error = (e.detail[5:-17]).split(")=(")
        values = filtered_error[1].split(", ")
        recovered_geometry = str(from_wkb(values[3]))
        found = int(df_edges[
                        (df_edges["u"] == int(values[0])) &
                        (df_edges["v"] == int(values[1])) &
                        (df_edges["type"] == values[2]) &
                        (df_edges["geometry"] == "SRID=4326; " + recovered_geometry) &
                        (df_edges["route"] == values[4])
                        ].iloc[0].name)
        df_edges.loc[found, "new_id"] = await self.select_one_by_unique_critique(
            int(values[0]), int(values[1]), values[2], recovered_geometry, values[4]
        )
        df = df.drop(index=found)
        
        return df_edges, df
    
    async def select_one(self, _id: int) -> EdgeEntity:
        """Select one edge by id.
        
        Args:
            _id (int): id of edge.
        Returns:
            EdgeEntity: edge object.
        """
        
        statement = (
            select(
                edges.c.id,
                edges.c.u,
                edges.c.v,
                edges.c.type,
                edges.c.weight,
                edges.c.weight_type,
                edges.c.level,
                edges.c.speed,
                edges.c.properties,
                cast(geofunc.ST_AsGeoJSON(edges.c.geometry), JSONB).label("geometry"),
                edges.c.created_at,
                edges.c.updated_at,
            )
            .select_from(edges)
            .where(edges.c.id == _id)
        )
        
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if not result:
            raise HTTPException(404, "EDGE_NOT_FOUND")
        return EdgeEntity(**result)
    
    async def select_one_by_geometry(self, geometry: geom.LineString) -> tuple[int | None, AsyncSession]:
        statement = (
            select(
                edges.c.id
            )
            .select_from(edges)
            .where(geofunc.ST_Equals(
                geofunc.ST_GeomFromText(str(geometry), text("4326")),
                edges.c.geometry
            ))
        )
        
        result, session = await self.database.execute_with_commit(statement)
        result = result.mappings().one_or_none()
        return result["id"] if result else result, session
    
    async def select_one_by_unique_critique(self, u: int, v: int, _type: str, geometry: str, route: str) -> int:
        statement = (
            select(edges.c.id)
            .select_from(edges)
            .where(edges.c.u == u)
            .where(edges.c.v == v)
            .where(edges.c.type == _type)
            .where(geofunc.ST_OrderingEquals(geofunc.ST_GeomFromText(str(geometry), text("4326")), edges.c.geometry))
            .where(edges.c.route == route)
        )
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if result is None:
            raise HTTPException(404, "NODE_NOT_FOUND")
        return result["id"]
    
    async def select_many(self, dto: SelectEdgesDTO) -> list[EdgeEntity]:
        """Select multiple edges by graph, type, level or geometry.
        
        Args:
            dto: DTO object.
        Returns:
            list[EdgeEntity]: list of edges.
        """
        
        statement = (
            select(
                edges.c.id,
                edges.c.u,
                edges.c.v,
                edges.c.type,
                edges.c.weight,
                edges.c.weight_type,
                edges.c.level,
                edges.c.speed,
                edges.c.route,
                edges.c.properties,
                cast(geofunc.ST_AsGeoJSON(edges.c.geometry), JSONB).label("geometry"),
                edges.c.created_at,
                edges.c.updated_at,
            )
            .select_from(edges)
        )
        
        if dto.graph:
            statement = (
                statement
                .join(graph_edges, edges.c.id == graph_edges.c.edge, isouter=True)
                .where(graph_edges.c.graph == dto.graph))
        if dto.type:
            statement = statement.where(edges.c.type == dto.type)
        if dto.level:
            statement = statement.where(edges.c.level == dto.level)
        if dto.geometry:
            statement = (
                statement.where(geofunc.ST_Intersects(
                    geofunc.ST_GeomFromText(str(dto.geometry.as_shapely_geometry()), text("4326")),
                    edges.c.geometry
                ))
            )
        
        results = (await self.database.execute_query(statement)).mappings().all()
        return [EdgeEntity(**result) for result in results]
    
    async def delete_edge(self, edge_id: int):

        statement = (
            delete(edges).where(edges.c.id == edge_id)
        )
        await self.database.execute_query(statement)
