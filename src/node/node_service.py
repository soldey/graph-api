import hashlib
from pathlib import Path

from asyncpg import UniqueViolationError
from fastapi import HTTPException
from iduconfig import Config
from loguru import logger
from pandas import DataFrame
from pandas.core.util.hashing import hash_pandas_object
from shapely import from_wkb
from sqlalchemy import insert, text, cast, select, or_, delete, distinct
from geoalchemy2 import functions as geofunc
from sqlalchemy.dialects.postgresql import JSONB
import shapely.geometry as geom
from sqlalchemy.ext.asyncio import AsyncSession

from src.common.db.database import DatabaseModule
from src.common.db.entities.edges import edges
from src.common.db.entities.graph_edges import graph_edges
from src.common.db.entities.nodes import nodes
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
    
    async def create_many(self, df_nodes: DataFrame) -> DataFrame:
        
        logger.info("Starting bulk nodes upload")
        
        df_nodes["point"] = df_nodes["point"].apply(lambda x: "SRID=4326; " + str(x.as_shapely_geometry()))
        df_nodes["type"] = df_nodes["type"].apply(lambda x: x.value)
        df = df_nodes.copy()
        
        df_hash = hashlib.sha256(df['point'].values).hexdigest()
        csv_name = f"nodes_{df_hash[:12]}.csv"
        
        df_nodes["new_id"] = [None] * len(df_nodes)
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(self.csv_dir / csv_name, sep="&", header=False, lineterminator="\n", index=False)
            try:
                res = [record["id"] for record in await self.database.execute_copy("nodes", str(self.csv_dir / csv_name), columns)]
                logger.success(f"Successfully added {len(res)} nodes")
                
                j = 0
                for i in range(len(df_nodes)):
                    if df_nodes["new_id"][i] is None:
                        df_nodes.loc[i, "new_id"] = res[j]
                        j += 1
                
                done = True
            except UniqueViolationError as e:
                logger.error(e)
                df_nodes, df = await self._conflict_resolver(e, df_nodes, df)
        (self.csv_dir / csv_name).unlink()
        return df_nodes
    
    async def _conflict_resolver(self, e: Exception, df_nodes: DataFrame, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        TODO: conflict resolver for existing nodes, more research towards returned geometry type (hash?)
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
            statement = statement.where(geofunc.ST_Covers(
                geofunc.ST_GeomFromText(str(dto.geometry.as_shapely_geometry()), text("4326")),
                nodes.c.point
            ))
        results = (await self.database.execute_query(statement)).mappings().all()
        return [NodeEntity(**result) for result in results]
    
    async def delete_node(self, node_id: int):

        statement = (
            delete(nodes).where(nodes.c.id == node_id)
        )
        await self.database.execute_query(statement)
