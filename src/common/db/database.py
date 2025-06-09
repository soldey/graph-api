from enum import Enum
from io import StringIO, BytesIO
from pathlib import Path

from asyncpg import Record
from iduconfig import Config
from loguru import logger
from sqlalchemy import Executable, Result, select, NullPool, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import URL
from sqlalchemy.orm import sessionmaker
from asyncpg.connection import Connection


class DatabaseModule:
    def __init__(self, config: Config):
        url = URL.create(
            drivername=config.get("DB_DRIVER"),
            host=config.get("DB_HOST"),
            port=int(config.get("DB_PORT")),
            username=config.get("DB_USER"),
            password=config.get("DB_PASSWORD"),
            database=config.get("DB_DATABASE"),
        )
        self.engine = create_async_engine(url, echo=True, poolclass=NullPool)
        self._session_maker = sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=True)

    async def execute_query(self, query: Executable) -> Result:
        """
        Wrapper for executing SQL query

        :param query: Executable query from SQLAlchemy ORM
        :return: Result of SQL query
        """

        async with self._session_maker() as session:
            return await session.execute(query)

    async def execute_with_commit(self, query: Executable) -> tuple[Result, AsyncSession]:
        """
        Wrapper for executing SQL query with delayed commit

        :param query: Executable query from SQLAlchemy ORM
        :return: Result of SQL query and session to be committed and closed
        """
        session = self._session_maker()
        result = await session.execute(query)
        return result, session
    
    @staticmethod
    async def execute_with_session(query: Executable, session: AsyncSession) -> tuple[Result, AsyncSession]:
        """Wrapper for continuous transaction query call.
        
        Args:
            query (Executable): ORM query.
            session (AsyncSession): existing session.

        Returns:
            Result of SQL query and session to be committed and closed
        """
        result = await session.execute(query)
        return result, session
    
    async def execute_copy(self, table_name: str, csv_name: str, columns: list[str]) -> list[Record]:
        """Driver specific method for executing COPY FROM method.
        
        Args:
            table_name (str): table name.
            csv_name (str): csv filename.
            columns (list[str]): columns to upload into.
        Returns:
            list[Record]: list of Record ids.
        """
        
        async with self._session_maker() as session:
            session: AsyncSession = session
            conn: Connection = (await (await session.connection()).get_raw_connection()).driver_connection
            
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.copy_to_table(
                    table_name=table_name, source=csv_name, columns=columns, delimiter="&"
                )
                res = await conn.fetch(
                    f"""
                    SELECT id FROM {table_name}
                    WHERE xmin::text = (txid_current() % (2^32)::bigint)::text
                    ORDER BY id ASC
                    """
                )
                await tr.commit()
                return res
            except Exception as e:
                await tr.rollback()
                raise e

    async def verify_connection(self):
        async with self.engine.connect() as conn:
            await conn.execute(select(1))
