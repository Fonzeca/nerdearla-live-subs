"""Gateway: sirve las páginas y reparte los subtítulos por WebSocket.

No guarda estado: se puede correr con N réplicas detrás de un balanceador.
"""
import json
import os
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
HISTORY = 200  # entradas del stream que se reenvían al conectar
STATIC = Path(__file__).parent.parent / "static"

r = aioredis.from_url(REDIS_URL, decode_responses=True, socket_timeout=30)  # > block de XREAD
app = FastAPI()


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/s/{sid}")
async def viewer(sid: str):
    return FileResponse(STATIC / "viewer.html")


@app.get("/api/sessions")
async def sessions():
    return sorted(await r.smembers("sessions"))


@app.get("/health")
async def health():
    await r.ping()
    return {"ok": True}


def msg(id_, fields):
    return json.dumps({"id": id_, **json.loads(fields["d"])}, ensure_ascii=False)


@app.websocket("/ws/{sid}")
async def ws(ws: WebSocket, sid: str, last: str | None = None):
    """?last=<id> retoma desde ese mensaje (reconexión sin perder líneas)."""
    await ws.accept()
    key = f"subs:{sid}"
    try:
        if last:
            entries = await r.xrange(key, min=f"({last}", max="+")
        else:
            entries = list(reversed(await r.xrevrange(key, count=HISTORY)))
        for id_, f in entries:
            await ws.send_text(msg(id_, f))
        cursor = entries[-1][0] if entries else (last or "0-0")
        while True:
            res = await r.xread({key: cursor}, block=15000, count=100)
            if not res:
                await ws.send_text('{"type":"ping"}')  # detecta clientes caídos
                continue
            for id_, f in res[0][1]:
                await ws.send_text(msg(id_, f))
                cursor = id_
    except (WebSocketDisconnect, RuntimeError):
        pass
