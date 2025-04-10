import hashlib
from io import BytesIO
from pathlib import Path

import networkx as nx
import pandas as pd
from asyncpg import UniqueViolationError
from fastapi import HTTPException
from geopandas import GeoDataFrame
from loguru import logger
from matplotlib.pyplot import savefig, clf
from networkx import MultiDiGraph
from pandas import DataFrame, MultiIndex, notna
from sqlalchemy import insert, select, delete

from src.graph.dto.select_graph_with_edges_dto import SelectGraphWithEdgesDTO
from src.common.db.database import DatabaseModule
from src.common.db.entities.graph_edges import graph_edges
from src.common.db.entities.graphs import graphs
from src.edge.dto.create_edge_dto import CreateEdgeDTO
from src.edge.dto.select_edges_dto import SelectEdgesDTO
from src.edge.edge_entity import EdgeEntity
from src.edge.edge_service import EdgeService
from src.graph.dto.create_graph_dto import CreateGraphDTO
from src.graph.dto.select_graphs_dto import SelectGraphsDTO
from src.graph.graph_edge_entity import GraphEdgeEntity
from src.graph.graph_entity import GraphEntity
from src.node.dto.create_node_dto import CreateNodeDTO
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
    
    async def add_edges(self, dtos: list[CreateEdgeDTO]):
        return [await self.add_edge(dto) for dto in dtos]
    
    async def create_many(self, graph: int, edges: list[int]):
        df = DataFrame.from_dict(data={"graph": graph, "edge": edges})
        
        df_hash = hashlib.sha256(df['edge'].values).hexdigest()
        csv_name = f"{df_hash[:12]}.csv"
        
        columns = df.columns.to_list()
        done = False
        while not done:
            df.to_csv(Path().absolute() / csv_name, sep=",", header=False, lineterminator="\n", index=False)
            try:
                res = [record["id"] for record in await self.database.execute_copy("graph_edges", csv_name, columns)]
                logger.success(f"Successfully added {len(res)} relationships")
                
                done = True
            except UniqueViolationError as e:
                df = await self._conflict_resolver(e, df)
                
                logger.error(e)
        (Path().absolute() / csv_name).unlink()
        df["new_id"] = res
        return df
    
    async def _conflict_resolver(self, e: Exception, df: DataFrame) -> DataFrame:
        """
        TODO: conflict resolver for existing nodes, more research towards returned geometry type (hash?)
        """
        
        filtered_error = (e.detail[5:-17]).split(")=(")
        values = filtered_error[1].split(", ")
        found = int(df[
                (df["graph"] == int(values[0])) &
                (df["edge"] == int(values[1]))
                ].iloc[0].name)
        df = df.drop(index=found)
        
        return df
    
    async def bulk_graph_upload(self, nodes: list[CreateNodeDTO], edges: list[CreateEdgeDTO], graph: int):
        df_nodes = DataFrame(data=[dto.__dict__ for dto in nodes])
        df_nodes = await self.node_service.create_many(df_nodes)
        new_nodes_id = df_nodes["new_id"].to_dict()
        
        df_edges = DataFrame(data=[dto.__dict__ for dto in edges])
        df_edges["u"] = df_edges["u"].map(new_nodes_id)
        df_edges["v"] = df_edges["v"].map(new_nodes_id)
        df_edges = await self.edge_service.create_many(df_edges)

        ships = await self.create_many(graph, df_edges["new_id"])
        return ships
    
    async def build_nx_graph(self, dto: SelectGraphWithEdgesDTO) -> MultiDiGraph:
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
        
        if gdf_nodes.active_geometry_name is None:
            df_nodes = DataFrame(gdf_nodes)
        else:
            df_nodes = gdf_nodes.drop(columns=gdf_nodes.active_geometry_name)
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
        index = MultiIndex.from_tuples([(edges[i].u, edges[i].v, keys[i]) for i in range(len(edges))])
        gdf_edges = GeoDataFrame(dict_edges, crs="EPSG:4326")
        gdf_edges.index = index
        gdf_edges.drop(columns=["u", "v", "key"], inplace=True)
        logger.info("Combined edges into gdf")
        
        if graph is not None:
            graph_attrs = {
                "crs": gdf_edges.crs,
                **graph.__dict__
            }
        else:
            graph_attrs = {
                "crs": gdf_edges.crs
            }
        G = MultiDiGraph(**graph_attrs)
        logger.info("Built graph without attributes")
        
        attr_names = gdf_edges.columns.tolist()
        for (u, v, k), attr_vals in zip(gdf_edges.index, gdf_edges.to_numpy()):
            data_all = zip(attr_names, attr_vals)
            data = {name: val for name, val in data_all if isinstance(val, list) or notna(val)}
            G.add_edge(u, v, key=k, **data)
        
        for col in df_nodes.columns:
            nx.set_node_attributes(G, name=col, values=df_nodes[col].dropna())
        
        logger.info("Finished building graph")
        return G
    
    async def visualize_graph(self, dto: SelectGraphWithEdgesDTO) -> BytesIO:
        
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
        edges = await self.edge_service.select_many(SelectEdgesDTO(
            graph=None if graph is None else graph.id,
            geometry=dto.geometry,
        ))
        nodes_set = set()
        for edge in edges:
            nodes_set.add(await self.node_service.select_one(edge.u))
            nodes_set.add(await self.node_service.select_one(edge.v))
        return graph, edges, list(nodes_set)

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
