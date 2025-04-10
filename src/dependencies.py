import inspect
import logging
import sys
from datetime import datetime

from iduconfig import Config
from loguru import logger

from src.edge.edge_service import EdgeService
from src.graph.graph_service import GraphService
from src.node.node_service import NodeService
from src.common.db.database import DatabaseModule

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}.{function}</cyan> - "
    "<level>{message}</level>",
    level="INFO",
    colorize=True,
)

config = Config()

logger.add(
    f"{config.get('LOGS_DIR')}/{datetime.now().strftime('%Y-%m-%d-%H-%M')}.log",
    format="<green>{time:MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}.{function}</cyan> - "
    "<level>{message}</level>",
)


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if any(lib in record.name for lib in ["importlib", "_bootstrap", "matplotlib", "geopandas"]):
            return
        
        # Get corresponding Loguru level if it exists.
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame:
            filename = frame.f_code.co_filename
            is_logging = filename == logging.__file__
            is_frozen = any(lib in filename for lib in ["importlib", "_bootstrap", "matplotlib", "geopandas"])
            if depth > 0 and not (is_logging or is_frozen):
                break
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

database = DatabaseModule(config)
node_service = NodeService(config, database)
edge_service = EdgeService(config, database, node_service)
graph_service = GraphService(database, node_service, edge_service)
