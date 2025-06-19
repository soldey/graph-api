import hashlib
import json
from pathlib import Path

import numpy
import shapely.geometry as geom
from asyncpg import UniqueViolationError
from fastapi import HTTPException
from geoalchemy2 import functions as geofunc
from iduconfig import Config
from loguru import logger
from numpy.f2py.crackfortran import include_paths
from pandas import DataFrame
from shapely import from_wkb
from shapely.geometry.base import BaseGeometry
from sqlalchemy import insert, text, cast, select, or_, delete, distinct
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.db.database import DatabaseModule
from src.common.db.entities.edges import edges
from src.common.db.entities.graph_edges import graph_edges
from src.common.db.entities.nodes import nodes
from src.common.geometries import Geometry
from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.dto.select_nodes_dto import SelectNodesDTO
from src.node.node_entity import NodeEntity


class NodeService:
    def __init__(self, config: Config, database: DatabaseModule):
        self.database = database
        self.csv_dir = Path().absolute() / config.get("CSV_DIR")
        self.csv_dir.mkdir(exist_ok=True)

    async def create(self, dto: CreateNodeDTO) -> NodeEntity:
        """Create one node.

        Args:
            dto (CreateNodeDTO): DTO object.
        Returns:
            NodeEntity: node object.
        """
        
        logger.info("Started creating node")
        point, session = await self.select_one_by_geometry(dto.point.as_shapely_geometry())
        if point is not None:
            await session.close()
            logger.info("Found existing node")
            return await self.select_one(point)

        statement = (
            insert(nodes).values(
                type=dto.type,
                properties=dto.properties,
                point=geofunc.ST_GeomFromText(str(dto.point.as_shapely_geometry()), text("4326"))
            )
        ).returning(
            nodes.c.id,
            nodes.c.type,
            nodes.c.properties,
            nodes.c.route,
            cast(geofunc.ST_AsGeoJSON(nodes.c.point), JSONB).label("point"),
            nodes.c.created_at,
            nodes.c.updated_at,
        )

        result, session = await self.database.execute_with_session(statement, session)
        result = result.mappings().one()

        await session.commit()
        await session.close()

        logger.info("Node committed")
        return NodeEntity(**result)
    
    async def create_many(self, df_nodes: DataFrame, geometry: BaseGeometry) -> tuple[DataFrame, int]:
        """Bulk node creation.
        
        Args:
            df_nodes (DataFrame): entry data for nodes in DataFrame.
            geometry (BaseGeometry): any geometry to look existing nodes in.
        
        Returns:
            DataFrame: nodes DataFrame with "new_id" column.
        """
        
        logger.info("Checking for existing nodes")

        df_nodes["new_id"] = [None] * len(df_nodes)
        df_nodes["point"] = df_nodes["point"].apply(lambda x: str(x.as_shapely_geometry()))
        df_nodes["type"] = df_nodes["type"].apply(lambda x: x.value)
        
        existing_nodes = await self.select_many(SelectNodesDTO(geometry=Geometry.from_shapely_geometry(geometry)))
        if len(existing_nodes) != 0:
            
            existing_nodes_df = DataFrame(data=[node.__dict__ for node in existing_nodes])
            existing_nodes_df["point"] = existing_nodes_df["point"].apply(lambda x: str(x))
            existing_nodes_df["type"] = existing_nodes_df["type"].apply(lambda x: x.value)
            
            df_nodes = df_nodes.merge(
                existing_nodes_df,
                on=["type", "point", "route"],
                how="left"
            )
            df_nodes["new_id"] = df_nodes["id"]
            df_nodes["properties"] = df_nodes["properties_x"]
            df_nodes.drop(
                columns=[
                    "created_at",
                    "updated_at",
                    "properties_x",
                    "properties_y",
                    "id"
                ],
                inplace=True
            )
            df_nodes["point"] = df_nodes["point"].apply(lambda x: "SRID=4326; " + str(x))
            df = df_nodes.copy()
            df = df[df_nodes["new_id"].isna()]
            df.drop(columns=["new_id"], inplace=True)
        else:
            df_nodes["point"] = df_nodes["point"].apply(lambda x: "SRID=4326; " + str(x))
            df = df_nodes.copy()
            df.drop(columns=["new_id"], inplace=True)

        df["properties"] = df["properties"].apply(lambda x: json.dumps(x))
        
        logger.info("Starting bulk nodes upload")
        
        df_hash = hashlib.sha256(df['point'].values).hexdigest()
        csv_name = f"nodes_{df_hash[:12]}.csv"
        
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(self.csv_dir / csv_name, sep="&", header=False, lineterminator="\n", index=False)
            try:
                res = [record["id"] for record in await self.database.execute_copy("nodes", str(self.csv_dir / csv_name), columns)]
                logger.success(f"Successfully added {len(res)} nodes")
                
                mask = df_nodes["new_id"].isna()
                df_nodes.loc[mask, "new_id"] = res[:mask.sum()]
                # await self.verify_correctness(df_nodes)
                
                done = True
            except UniqueViolationError as e:
                logger.error(e)
                df_nodes, df = await self._conflict_resolver(e, df_nodes, df)
        (self.csv_dir / csv_name).unlink()
        df_nodes["new_id"] = df_nodes["new_id"].astype(int)
        return df_nodes, len(res)
    
    async def _conflict_resolver(self, e: Exception, df_nodes: DataFrame, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Conflict resolver for bulk upload.
        
        Args:
            e (Exception): an object of UniqueViolationError from asyncpg, containing detail of duplicated entity.
            df_nodes (DataFrame): entry data for nodes in DataFrame to find duplicated entity.
            df (DataFrame): mutable entry data for nodes to drop duplicated entity from.
        
        Returns:
            tuple[DataFrame, DataFrame]: df_nodes, df respectively.
        """
        
        filtered_error = (e.detail[5:-17]).split(")=(")
        values = filtered_error[1].split(", ")
        recovered_geometry = str(from_wkb(values[1]))
        found = int(df_nodes[
                        (df_nodes["type"] == values[0]) &
                        (df_nodes["point"] == "SRID=4326; " + recovered_geometry) &
                        (df_nodes["route"] == values[2])
                        ].iloc[0].name)
        df_nodes.loc[found, "new_id"] = await self.select_one_by_unique_critique(
            values[0], recovered_geometry, values[2]
        )
        df = df.drop(index=found)
        
        return df_nodes, df
    
    async def verify_correctness(self, df_nodes: DataFrame):
        ids = df_nodes["new_id"].astype(int).tolist()
        df_nodes["id"] = df_nodes["new_id"]
        df_nodes.drop(columns=["new_id"], inplace=True)
        results = []
        for _id in ids:
            result = await self.select_one(_id)
            results.append(result)
        df_from_db = DataFrame(data=[result.__dict__ for result in results])
        
        df_nodes = df_nodes.merge(df_from_db, on=["id"])
        df_nodes["point_y"] = df_nodes["point_y"].apply(lambda x: "SRID=4326; " + str(x))
        df_nodes["equal"] = numpy.equal(df_nodes["point_x"], df_nodes["point_y"])
        return df_nodes

    async def select_one(self, _id: int) -> NodeEntity:
        """Select one node by id.

        Args:
            _id (int): id of node.
        Returns:
            NodeEntity: node object.
        Raises:
            HTTPException: 404 if node was not found.
        """

        statement = (
            select(
                nodes.c.id,
                nodes.c.type,
                nodes.c.properties,
                nodes.c.route,
                cast(geofunc.ST_AsGeoJSON(nodes.c.point), JSONB).label("point"),
                nodes.c.created_at,
                nodes.c.updated_at,
            )
            .select_from(nodes)
            .where(nodes.c.id == _id)
        )
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if not result:
            raise HTTPException(404, "NODE_NOT_FOUND")
        return NodeEntity(**result)
    
    async def select_one_by_unique_critique(self, _type: str, point: str, route: str) -> int:
        """Select one node by unique critique (type, point, route).
        
        Args:
            _type (str): type of node.
            point (str): string representation of point geometry.
            route (str): route if node is transport node.
        
        Returns:
            int: node id.
        Raises:
            HTTPException(404): if node wasn't found.
        """
        
        statement = (
            select(nodes.c.id)
            .select_from(nodes)
            .where(nodes.c.type == _type)
            .where(geofunc.ST_Equals(geofunc.ST_GeomFromText(str(point), text("4326")), nodes.c.point))
            .where(nodes.c.route == route)
        )
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if result is None:
            raise HTTPException(404, "NODE_NOT_FOUND")
        return result["id"]
    
    async def select_one_by_geometry(self, geometry: geom.Point) -> tuple[int | None, AsyncSession]:
        """Select one node by geometry.
        
        Args:
            geometry (Point): Point geometry.
        
        Returns:
            tuple[Optional[int], AsyncSession]: node id and session to commit respectively.
        """
        
        statement = (
            select(
                nodes.c.id
            )
            .select_from(nodes)
            .where(geofunc.ST_Equals(
                geofunc.ST_GeomFromText(str(geometry), text("4326")),
                nodes.c.point
            ))
        )
        
        result, session = await self.database.execute_with_commit(statement)
        result = result.mappings().one_or_none()
        return result["id"] if result else result, session

    async def select_many(self, dto: SelectNodesDTO) -> list[NodeEntity]:
        """Select multiple nodes by type and/or geometry.

        Args:
            dto (SelectNodesDTO): DTO object.
        Returns:
            list[NodeEntity]: list of nodes.
        """
        
        statement = (
            select(
                distinct(nodes.c.id),
                nodes.c.type,
                nodes.c.properties,
                nodes.c.route,
                cast(geofunc.ST_AsGeoJSON(nodes.c.point), JSONB).label("point"),
                nodes.c.created_at,
                nodes.c.updated_at,
            )
            .select_from(nodes)
        )
        if dto.graph is not None:
            statement = (
                statement.join(edges, or_(nodes.c.id == edges.c.u, nodes.c.id == edges.c.v))
                .join(graph_edges, edges.c.id == graph_edges.c.edge)
                .where(graph_edges.c.graph == dto.graph)
            )
        if dto.type:
            statement = statement.where(nodes.c.type == dto.type)
        if dto.geometry:
            statement = statement.where(geofunc.ST_Intersects(
                geofunc.ST_GeomFromText(str(dto.geometry.as_shapely_geometry()), text("4326")),
                nodes.c.point
            ))
        results = (await self.database.execute_query(statement)).mappings().all()
        return [NodeEntity(**result) for result in results]
    
    async def delete_node(self, node_id: int):
        """Delete existing node by id.
        
        Args:
            node_id (int): node id.
        """

        statement = (
            delete(nodes).where(nodes.c.id == node_id)
        )
        await self.database.execute_query(statement)
