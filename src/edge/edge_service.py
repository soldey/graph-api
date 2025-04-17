import csv
import hashlib
from pathlib import Path

import shapely.geometry as geom
from asyncpg import UniqueViolationError
from fastapi import HTTPException
from geoalchemy2 import functions as geofunc
from iduconfig import Config
from loguru import logger
from pandas import DataFrame
from shapely import from_wkb
from shapely.geometry.base import BaseGeometry
from sqlalchemy import insert, cast, text, select, delete, or_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.db.database import DatabaseModule
from src.common.db.entities.edges import edges
from src.common.db.entities.graph_edges import graph_edges
from src.common.db.entities.nodes import nodes
from src.common.geometries import Geometry
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.edge.dto.select_edges_dto import SelectEdgesDTO
from src.edge.edge_entity import EdgeEntity
from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.node_service import NodeService


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
                route=dto.route,
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
            edges.c.route,
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
    
    async def create_many(self, df_edges: DataFrame, geometry: BaseGeometry) -> DataFrame:
        """Bulk edge creation.
        
        Args:
            df_edges (DataFrame): entry data for edges in DataFrame.
            geometry (BaseGeometry): any geometry to find existing edges in.

        Returns:
            DataFrame: edges DataFrame with "new_id" column.
        """
        
        df_edges["new_id"] = [None] * len(df_edges)
        df_edges["type"] = df_edges["type"].apply(lambda x: x.value)
        df_edges["weight_type"] = df_edges["weight_type"].apply(lambda x: x.value)
        df_edges["level"] = df_edges["level"].apply(lambda x: x.value)
        df_edges["route"] = df_edges["route"].fillna("")
        df_edges["geometry"] = df_edges["geometry"].apply(lambda x: str(x))
        
        existing_edges = await self.select_many(SelectEdgesDTO(geometry=Geometry.from_shapely_geometry(geometry)))
        if len(existing_edges) != 0:
            existing_edges_df = DataFrame(data=[node.__dict__ for node in existing_edges])
            existing_edges_df["type"] = existing_edges_df["type"].apply(lambda x: x.value)
            existing_edges_df["weight_type"] = existing_edges_df["weight_type"].apply(lambda x: x.value)
            existing_edges_df["level"] = existing_edges_df["level"].apply(lambda x: x.value)
            existing_edges_df["geometry"] = existing_edges_df["geometry"].apply(lambda x: str(x))
            
            df_edges = df_edges.merge(
                existing_edges_df,
                on=["u", "v", "type", "geometry", "route"],
                how="left"
            )
            df_edges["new_id"] = df_edges["id"]
            df_edges["properties"] = df_edges["properties_x"]
            df_edges["weight"] = df_edges["weight_x"]
            df_edges["weight_type"] = df_edges["weight_type_x"]
            df_edges["level"] = df_edges["level_x"]
            df_edges["speed"] = df_edges["speed_x"]
            df_edges.drop(
                columns=[
                    "created_at",
                    "updated_at",
                    "properties_y",
                    "weight_y",
                    "weight_type_y",
                    "level_y",
                    "speed_y",
                    "properties_x",
                    "weight_x",
                    "weight_type_x",
                    "level_x",
                    "speed_x",
                    "id"
                ],
                inplace=True
            )
            df_edges["geometry"] = df_edges["geometry"].apply(lambda x: "SRID=4326; " + str(x))
            df = df_edges.copy()
            df = df[df_edges["new_id"].isna()]
            df.drop(columns=["new_id"], inplace=True)
        else:
            df_edges["geometry"] = df_edges["geometry"].apply(lambda x: "SRID=4326; " + str(x))
            df = df_edges.copy()
            df.drop(columns=["new_id"], inplace=True)
        
        logger.info("Starting bulk edge upload")

        
        df_hash = hashlib.sha256(df['geometry'].values).hexdigest()
        csv_name = f"edges_{df_hash[:12]}.csv"
        
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(self.csv_dir / csv_name, sep="&", header=False, lineterminator="\n", index=False, quoting=csv.QUOTE_NONE, quotechar='', escapechar='\\')
            try:
                res = [record["id"] for record in await self.database.execute_copy("edges", str(self.csv_dir / csv_name), columns)]
                logger.success(f"Successfully added {len(res)} edges")
                
                mask = df_edges["new_id"].isna()
                df_edges.loc[mask, "new_id"] = res[:mask.sum()]
                
                done = True
            except UniqueViolationError as e:
                logger.error(e)
                df_edges, df = await self._conflict_resolver(e, df_edges, df)
        
        (self.csv_dir / csv_name).unlink()
        df_edges["new_id"] = df_edges["new_id"].astype(int)
        return df_edges
    
    async def _conflict_resolver(self, e: Exception, df_edges: DataFrame, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Conflict resolver for bulk upload.
        
        Args:
            e (Exception): an object of UniqueViolationError from asyncpg, containing detail of duplicated entity.
            df_edges (DataFrame): entry data for edges in DataFrame to find duplicated entity.
            df (DataFrame): mutable entry data for edges to drop duplicated entity from.
        
        Returns:
            tuple[DataFrame, DataFrame]: df_edges, df respectively.
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
        try:
            df_edges.loc[found, "new_id"] = await self.select_one_by_unique_critique(
                int(values[0]), int(values[1]), values[2], recovered_geometry, values[4]
            )
        except HTTPException as e:
            df_edges = df_edges.drop(index=found)
        df = df.drop(index=found)
        
        return df_edges, df
    
    async def select_one(self, _id: int) -> EdgeEntity:
        """Select one edge by id.
        
        Args:
            _id (int): id of edge.
        Returns:
            EdgeEntity: edge object.
        Raises:
            HTTPException(404): if edge wasn't found.
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
            .where(edges.c.id == _id)
        )
        
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if not result:
            raise HTTPException(404, "EDGE_NOT_FOUND")
        return EdgeEntity(**result)
    
    async def select_one_by_geometry(self, geometry: geom.LineString) -> tuple[int | None, AsyncSession]:
        """Select one edge by geometry.
        
        Args:
            geometry (LineString): LineString geometry.
        
        Returns:
            tuple[Optional[int], AsyncSession]: edge id and session to commit respectively.
        """
        
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
        """Select one edge by unique critique (u, v, type, geometry, route).
        
        Args:
            u (int): source node id.
            v (int): destination node id.
            _type (str): type of edge.
            geometry (str): string representation of geometry.
            route (str): route if edge is transport edge.
        
        Returns:
            int: edge id.
        Raises:
            HTTPException(404): if edge wasn't found.
        """
        
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
                statement.join(nodes, or_(nodes.c.id == edges.c.u, nodes.c.id == edges.c.v))
                .where(geofunc.ST_Intersects(
                    geofunc.ST_GeomFromText(str(dto.geometry.as_shapely_geometry()), text("4326")),
                    nodes.c.point
                ))
            )
        
        results = (await self.database.execute_query(statement)).mappings().all()
        return [EdgeEntity(**result) for result in results]
    
    async def delete_edge(self, edge_id: int):
        """Delete existing edge by id.
        
        Args:
            edge_id (int): edge id.
        """

        statement = (
            delete(edges).where(edges.c.id == edge_id)
        )
        await self.database.execute_query(statement)
