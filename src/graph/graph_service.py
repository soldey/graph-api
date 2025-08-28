import asyncio
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
from asyncpg import UniqueViolationError
from fastapi import HTTPException
from geoalchemy2 import functions as geofunc
from geopandas import GeoDataFrame
from loguru import logger
from matplotlib.pyplot import savefig, clf
from networkx import MultiDiGraph
from pandas import DataFrame
from shapely.geometry.base import BaseGeometry
from shapely.geometry.geo import box
from sqlalchemy import insert, select, delete, text
from sqlalchemy.orm import aliased

from src.common.db.database import DatabaseModule
from src.common.db.entities.edges import edges, EdgeTypeEnum
from src.common.db.entities.graph_edges import graph_edges
from src.common.db.entities.graphs import graphs
from src.common.db.entities.nodes import nodes, NodeTypeEnum
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.edge.dto.select_edges_dto import SelectEdgesDTO
from src.edge.edge_entity import EdgeEntity
from src.edge.edge_service import EdgeService
from src.graph.dto.create_graph_dto import CreateGraphDTO
from src.graph.dto.select_graph_with_edges_dto import SelectGraphWithEdgesDTO
from src.graph.dto.select_graphs_dto import SelectGraphsDTO
from src.graph.graph_edge_entity import GraphEdgeEntity
from src.graph.graph_entity import GraphEntity
from src.node.dto.create_node_dto import CreateNodeDTO
from src.node.dto.select_nodes_dto import SelectNodesDTO
from src.node.node_entity import NodeEntity
from src.node.node_service import NodeService


class GraphService:
    def __init__(
            self,
            database: DatabaseModule,
            node_service: NodeService,
            edge_service: EdgeService,
    ):
        self.database = database
        self.edge_service = edge_service
        self.node_service = node_service

    async def create(self, dto: CreateGraphDTO) -> GraphEntity:
        """Create one graph.

        Args:
            dto (CreateGraphDTO): DTO object.
        Returns:
            GraphEntity: graph object without edges.
        Raises:
            HTTPException: 400 if name is numeric or 409 if graph with name is present in database.
        """
        
        logger.info("Started creating graph")
        if dto.name.isnumeric():
            raise HTTPException(400, "name can't be numeric")
        result = None
        try:
            result = await self.select_one(dto.name)
        except HTTPException:
            pass
        if result:
            logger.info("Found existing graph")
            raise HTTPException(409, "graph with such name already exists")

        statement = (
            insert(graphs).values(
                name=dto.name,
                type=dto.type,
                properties=dto.properties,
            ).returning(graphs)
        )

        result, session = await self.database.execute_with_commit(statement)
        result = result.mappings().one()

        await session.commit()
        await session.close()
        
        logger.info("Graph committed")
        return GraphEntity(**result)
    
    async def add_edge(self, dto: CreateEdgeDTO) -> GraphEdgeEntity:
        """Links existing edge to a graph or creates a new one.
        
        Args:
            dto (CreateEdgeDTO): DTO object.

        Returns:
            GraphEdgeEntity: graph relationship entity.
        Raises:
            HTTPException: 404 if graph wasn't found.
        """
        
        logger.info("Started adding edge")
        graph = await self.select_one(str(dto.graph))
        edge = await self.edge_service.create(dto)
        link = await self.select_one_relationship(graph.id, edge.id)
        if link:
            logger.info("Found existing relationship")
            return link
        
        statement = (
            insert(graph_edges).values(
                graph=graph.id,
                edge=edge.id
            ).returning(graph_edges)
        )
        
        result, session = await self.database.execute_with_commit(statement)
        result = result.mappings().one()
        
        await session.commit()
        await session.close()
        
        logger.info("Relationship committed")
        return GraphEdgeEntity(**result)
    
    async def create_many(self, graph: int, edges: list[int], geometry: Optional[BaseGeometry] = None) -> tuple[DataFrame, int]:
        """Bulk upload of relationships.
        
        Args:
            graph (int): graph id.
            edges (list[int]): list of uploaded edge ids.
            geometry (Optional[BaseGeometry]): geometry to look relationships in.
        
        Returns:
            DataFrame: filtered relationship DataFrame with "new_id" column.
        """
        
        df = DataFrame.from_dict(data={"graph": graph, "edge": edges})
        
        if geometry:
            existing_edges_ids = await self.select_many_edges_by_graph_and_geometry(graph, geometry)
            df = df[~df["edge"].isin(existing_edges_ids)]
        
        df_hash = hashlib.sha256(df['edge'].values).hexdigest()
        csv_name = f"{df_hash[:12]}.csv"
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(Path().absolute() / csv_name, sep="&", header=False, lineterminator="\n", index=False)
            try:
                res = [record["id"] for record in (await self.database.execute_copy("graph_edges", csv_name, columns))]
                logger.success(f"Successfully added {len(res)} relationships")
                
                done = True
            except UniqueViolationError as e:
                df = await self._conflict_resolver(e, df)
                
                logger.error(e)
        (Path().absolute() / csv_name).unlink()
        df["new_id"] = res
        return df, len(res)
    
    async def _conflict_resolver(self, e: Exception, df: DataFrame) -> DataFrame:
        """Conflict resolver for bulk upload.
        
        Args:
            e (Exception): an object of UniqueViolationError from asyncpg, containing detail of duplicated entity.
            df (DataFrame): entry data for relationships in DataFrame to find duplicated entity.
            df (DataFrame): mutable entry data for relationships to drop duplicated entity from.
        
        Returns:
            DataFrame: changed relationship DataFrame.
        """
        
        filtered_error = (e.detail[5:-17]).split(")=(")
        values = filtered_error[1].split(", ")
        found = int(df[
                (df["graph"] == int(values[0])) &
                (df["edge"] == int(values[1]))
                ].iloc[0].name)
        df = df.drop(index=found)
        
        return df
    
    async def bulk_graph_upload(self, nodes: list[CreateNodeDTO], edges: list[CreateEdgeDTO], graph: int) -> dict:
        """Bulk graph upload.
        
        Args:
            nodes (list[CreateNodeDTO]): entry data for nodes as DTO objects.
            edges (list[CreateEdgeDTO]): entry data for edges as DTO objects.
            graph (int): graph id to upload edges into.
        
        Returns:
            list[int]: list of uploaded relationship ids.
        """
        
        logger.info("Starting bulk graph upload")
        
        df_edges = DataFrame(data=[dto.__dict__ for dto in edges])
        df_edges["route"] = df_edges["route"].apply(lambda x: x[:min(200, len(x))].replace('\"', ''))
        if not df_edges.empty:
            df_edges.drop("graph", axis="columns", inplace=True)
            df_edges["geometry"] = df_edges["geometry"].apply(lambda x: x.as_shapely_geometry())
            gdf_edges = GeoDataFrame(df_edges, geometry="geometry", crs=4326)
            total_geometry = box(*gdf_edges.total_bounds)
            logger.info(f"Selected geometry - {total_geometry}")
        df_nodes = DataFrame(data=[dto.__dict__ for dto in nodes])
        df_nodes["route"] = df_nodes["route"].apply(lambda x: x[:min(200, len(x))].replace('\"', ''))
        if not df_edges.empty:
            df_nodes, nodes_uploaded = await self.node_service.create_many(df_nodes, total_geometry)
        else:
            df_nodes, nodes_uploaded = await self.node_service.create_many(df_nodes, None)
        new_nodes_id = df_nodes["new_id"].astype(int).to_dict()
        
        await asyncio.sleep(5)
        if not df_edges.empty:
            df_edges["u"] = df_edges["u"].map(new_nodes_id)
            df_edges["v"] = df_edges["v"].map(new_nodes_id)
            df_edges, edges_uploaded = await self.edge_service.create_many(df_edges, total_geometry)
    
            ships, ships_uploaded = await self.create_many(graph, df_edges["new_id"], total_geometry)
            return {
                "result": "success",
                "details": {
                    "nodes": {
                        "amount": nodes_uploaded,
                        "status": "uploaded",
                    },
                    "edges": {
                        "amount": edges_uploaded,
                        "status": "uploaded",
                    },
                    "relationships": {
                        "amount": ships_uploaded,
                        "status": "uploaded",
                    }
                }
            }
        else:
            return {
                "result": "success",
                "details": {
                    "nodes": {
                        "amount": nodes_uploaded,
                        "status": "uploaded",
                    },
                    "edges": {
                        "amount": 0,
                        "status": "uploaded",
                    },
                    "relationships": {
                        "amount": 0,
                        "status": "uploaded",
                    }
                }
            }
    
    async def build_nx_graph(self, dto: SelectGraphWithEdgesDTO) -> tuple[dict, GeoDataFrame, GeoDataFrame]:
        """Build multidimensional graph.
        
        Args:
            dto (SelectGraphWithEdgesDTO): DTO object.

        Returns:
            MultiDiGraph: networkx multidimensional graph.
        """
        
        logger.info("Started builing graph")
        graph, edges, nodes = await self.select_one_with_edges(dto)
        logger.info(f"Received graph with {len(edges)} edges and {len(nodes)} nodes")
        
        dict_nodes = {k if k != "point" else "geometry": [dic.__dict__[k] for dic in nodes] for k in nodes[0].__dict__}
        gdf_nodes = GeoDataFrame(dict_nodes, crs="EPSG:4326")
        gdf_nodes.set_index("id", drop=True, inplace=True)
        
        logger.info("Combined nodes into gdf and df")
        
        dict_edges = {k: [dic.__dict__[k] for dic in edges] for k in edges[0].__dict__}
        relationships = {}
        keys = []
        for edge in edges:
            if (edge.u, edge.v) not in relationships:
                relationships[(edge.u, edge.v)] = -1
            relationships[(edge.u, edge.v)] += 1
            keys.append(relationships[(edge.u, edge.v)])
        dict_edges["key"] = keys
        gdf_edges = GeoDataFrame(dict_edges, crs="EPSG:4326").reset_index(drop=True)
        logger.info("Combined edges into gdf")
        
        if graph is not None:
            graph_attrs = {
                "crs": str(gdf_edges.crs),
                **graph.__dict__
            }
        else:
            graph_attrs = {
                "crs": str(gdf_edges.crs)
            }
        gdf_edges["type"] = gdf_edges["type"].apply(lambda x: x.value)
        gdf_edges["weight_type"] = gdf_edges["weight_type"].apply(lambda x: x.value)
        gdf_edges["level"] = gdf_edges["level"].apply(lambda x: x.value)
        gdf_edges["route"] = gdf_edges["route"].fillna("")
        gdf_edges.drop(columns=["created_at", "updated_at"], inplace=True)


        gdf_nodes["type"] = gdf_nodes["type"].apply(lambda x: x.value)
        gdf_nodes.drop(columns=["created_at", "updated_at"], inplace=True)
        gdf_nodes.reset_index(names="node_id", inplace=True)
        logger.info("Finished building graph")
        return graph_attrs, gdf_edges, gdf_nodes
    
    async def visualize_graph(self, dto: SelectGraphWithEdgesDTO) -> BytesIO:
        """Visualization method for graph.
        
        Args:
            dto (SelectGraphWithEdgesDTO): DTO object.
        Returns:
            BytesIO: bytes for png.
        """
        
        logger.info("Started visualizing graph")
        graph, edges, nodes = await self.select_one_with_edges(dto)
        
        dict_nodes = {k if k != "point" else "geometry": [dic.__dict__[k] for dic in nodes] for k in nodes[0].__dict__}
        gdf_nodes = GeoDataFrame(dict_nodes, crs="EPSG:4326")
        gdf_nodes.set_index("id", drop=True, inplace=True)
        logger.info("Combined nodes into gdf")
        
        dict_edges = {k: [dic.__dict__[k] for dic in edges] for k in edges[0].__dict__}
        gdf_edges = GeoDataFrame(dict_edges, crs="EPSG:4326")
        
        gdf_edges.set_index("id", drop=True, inplace=True)
        logger.info("Combined edges into gdf")
        
        merged = pd.concat([gdf_edges, gdf_nodes])
        logger.info("Merged gdfs")
        merged.plot()
        bytesio = BytesIO()
        savefig(bytesio, format="png")
        clf()
        bytesio.seek(0)
        
        logger.info("Visualization finished")
        return bytesio
    
    async def select_many_edges_by_graph_and_geometry(self, graph: int, geometry: BaseGeometry) -> list[int]:
        """Select multiple edges through relationships by graph and geometry.
        
        Args:
            graph (int): graph id.
            geometry (BaseGeometry): geometry to find existing edges in.
        Returns:
            list[int]: list of edge ids.
        """
        node_u = aliased(nodes)
        node_v = aliased(nodes)
        
        statement = (
            select(graph_edges.c.edge)
            .select_from(graph_edges)
            .join(edges, edges.c.id == graph_edges.c.edge, isouter=True)
            .join(node_u, node_u.c.id == edges.c.u)
            .join(node_v, node_v.c.id == edges.c.v)
            .where(
                geofunc.ST_Intersects(
                    geofunc.ST_GeomFromText(str(geometry), text("4326")),
                    node_u.c.point
                ),
                geofunc.ST_Intersects(
                    geofunc.ST_GeomFromText(str(geometry), text("4326")),
                    node_v.c.point
                )
            )
            .where(graph_edges.c.graph == graph)
        )
        
        results = (await self.database.execute_query(statement)).mappings().all()
        return [result["edge"] for result in results]
    
    async def select_one_relationship(self, graph_id: int, edge_id: int) -> GraphEdgeEntity | None:
        """Select one graph-edge relationship.
        
        Args:
            graph_id (int): graph id.
            edge_id (int): edge id.

        Returns:
            GraphEdgeEntity: relationship entity.
        """
        
        statement = (
            select(
                graph_edges
            ).select_from(graph_edges)
            .where(graph_edges.c.graph == graph_id)
            .where(graph_edges.c.edge == edge_id)
        )
        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        return GraphEdgeEntity(**result) if result else result
    
    async def select_one_with_edges(self, dto: SelectGraphWithEdgesDTO) -> tuple[GraphEntity, list[EdgeEntity], list[NodeEntity]]:
        """Select graph with its edges and nodes.
        
        Args:
            dto (SelectGraphWithEdgesDTO): DTO object.

        Returns:
            GraphEntity, list[EdgeEntity], list[NodeEntity]: graph, list of edges, list of nodes.
        Raises:
            HTTPException: 404 if graph wasn't found.
        """
        if dto.id_or_name is not None:
            graph = await self.select_one(dto.id_or_name)
        else:
            graph = None
        if dto.type == "intermodal":
            selected_edge_types = [_type for _type in EdgeTypeEnum if _type != EdgeTypeEnum.WATERCHANNEL]
            selected_node_types = [_type for _type in NodeTypeEnum]
        elif dto.type == "water":
            selected_edge_types = EdgeTypeEnum.WATERCHANNEL
            selected_node_types = NodeTypeEnum.WALK
        elif dto.type == "drive":
            selected_edge_types = EdgeTypeEnum.DRIVE
            selected_node_types = NodeTypeEnum.DRIVE
        else:
            selected_edge_types = EdgeTypeEnum.WALK
            selected_node_types = NodeTypeEnum.WALK
        edges = await self.edge_service.select_many(SelectEdgesDTO(
            graph=None if graph is None else graph.id,
            geometry=dto.geometry,
            type=selected_edge_types,
        ))
        nodes = await self.node_service.select_many(SelectNodesDTO(
            graph=None if graph is None else graph.id,
            geometry=dto.geometry,
            type=selected_node_types,
        ))
        return graph, edges, nodes

    async def select_one(self, id_or_name: str) -> GraphEntity:
        """Select one graph.

        Args:
            id_or_name (str): id if numeric, name otherwise
        Returns:
            GraphEntity: graph object.
        Raises:
            HTTPException: 404 if graph wasn't found.
        """

        statement = (
            select(graphs)
            .select_from(graphs)
        )
        if id_or_name.isnumeric():
            statement = statement.where(graphs.c.id == int(id_or_name))
        else:
            statement = statement.where(graphs.c.name == id_or_name)

        result = (await self.database.execute_query(statement)).mappings().one_or_none()
        if not result:
            raise HTTPException(404, "GRAPH_NOT_FOUND")

        return GraphEntity(**result)

    async def select_many(self, dto: SelectGraphsDTO) -> list[GraphEntity]:
        """Select multiple graphs by type.

        Args:
            dto (SelectGraphsDTO): DTO object.
        Returns:
            list[GraphEntity]: list of graphs.
        """

        statement = (
            select(graphs)
            .select_from(graphs)
        )

        if dto.type:
            statement = statement.where(graphs.c.type == dto.type)

        results = (await self.database.execute_query(statement)).mappings().all()
        return [GraphEntity(**result) for result in results]
    
    async def delete_graph_edge(self, graph_edge_id: int):
        """Delete graph-edge relationship.
        
        Args:
            graph_edge_id (int): relationship id.
        """
        
        statement = (
            delete(graph_edges).where(graph_edges.c.id == graph_edge_id)
        )
        await self.database.execute_query(statement)
    
    async def delete_graph(self, graph_id: int):
        """Delete graph.
        
        Args:
            graph_id (int): graph id.
        """

        statement = (
            delete(graphs).where(graphs.c.id == graph_id)
        )
        await self.database.execute_query(statement)
