from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import Response, RedirectResponse

from src.dependencies import config

admin_router = APIRouter()
tag = ["Admin controller"]


@admin_router.get("/logs", tags=tag)
async def logs():
    files = [file.name for file in (Path().absolute() / config.get("LOGS_DIR")).glob("*.log")]
    files.sort(reverse=True)
    if len(files) == 0:
        return None
    with open(Path().absolute() / "logs" / files[0], "rb") as fin:
        return Response(
            content=fin.read(),
            media_type="text/plain"
        )


@admin_router.get("/", include_in_schema=False)
async def read_root():
    return RedirectResponse('/docs')
