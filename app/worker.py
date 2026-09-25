"""Worker: recibe audio por WebSocket, transcribe con faster-whisper,
traduce con Gemma (Ollama) y publica todo en Redis Streams."""
import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import numpy as np
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
from faster_whisper import WhisperModel
from faster_whisper.vad import VadOptions, get_speech_timestamps

from app.segmenter import SR, decide

log = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
TRANSLATE_MODEL = os.getenv("TRANSLATE_MODEL", "gemma3:4b")
TARGET_LANG = os.getenv("TARGET_LANG", "es")
TICK_S = float(os.getenv("TICK_S", "0.5"))
MAX_BACKLOG_S = 30
SID_RE = re.compile(r"[\w-]{1,64}")
STATIC = Path(__file__).parent.parent / "static"

model = WhisperModel(
    os.getenv("WHISPER_MODEL", "large-v3-turbo"),
    device=os.getenv("WHISPER_DEVICE", "cuda"),
    compute_type=os.getenv("WHISPER_COMPUTE", "float16"),
    num_workers=int(os.getenv("WHISPER_WORKERS", "2")),  # transcripciones en paralelo (1 por sala)
)
VAD = VadOptions(min_silence_duration_ms=300, speech_pad_ms=100)

r = aioredis.from_url(REDIS_URL, decode_responses=True)
http = httpx.AsyncClient(timeout=120)  # la 1ra llamada puede cargar el modelo
rooms: dict[str, "Room"] = {}
bg: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(app):
    # Precarga el modelo de traducción para que la 1ra frase no espere la carga.
    try:
        await http.post(f"{OLLAMA_URL}/api/generate", json={"model": TRANSLATE_MODEL, "keep_alive": -1})
    except Exception:
        log.warning("no se pudo precargar %s en %s", TRANSLATE_MODEL, OLLAMA_URL)
    yield


app = FastAPI(lifespan=lifespan)


class Room:
    def __init__(self, sid, lang, ws):
        self.sid, self.lang, self.ws = sid, lang, ws
        self.buf = np.zeros(0, np.float32)
        self.alive = True
        self.last_partial = ""


def transcribe(audio, lang):
    segs, info = model.transcribe(
        audio, language=lang, beam_size=1, without_timestamps=True,
        condition_on_previous_text=False,
    )
    # Filtra alucinaciones típicas de whisper sobre ruido ("Thank you.", etc.)
    text = " ".join(s.text.strip() for s in segs if s.no_speech_prob < 0.6).strip()
    return text, info.language, info.language_probability


async def emit(sid, **f):
    f["ts"] = time.time()
    await r.xadd(f"subs:{sid}", {"d": json.dumps(f, ensure_ascii=False)}, maxlen=5000, approximate=True)


async def translate(sid, seg, text, lang, t0):
    try:
        if lang != TARGET_LANG:
            resp = await http.post(f"{OLLAMA_URL}/api/generate", json={
                "model": TRANSLATE_MODEL, "stream": False, "keep_alive": -1,
                "options": {"temperature": 0},
                "prompt": f"Translate this conference transcript fragment from '{lang}' to '{TARGET_LANG}'. "
                          f"Reply with the translation only, no quotes or notes.\n\n{text}",
            })
            resp.raise_for_status()
            text = resp.json()["response"].strip()
        await emit(sid, type="tr", seg=seg, text=text, ms=int((time.time() - t0) * 1000))
    except Exception:
        log.exception("[%s] traducción falló (seg %s)", sid, seg)


async def process(room: Room):
    """Loop por sala: VAD sobre el buffer -> parcial / final / descartar."""
    sid = room.sid
    while True:
        await asyncio.sleep(TICK_S)
        if len(room.buf) > MAX_BACKLOG_S * SR:  # GPU no da abasto: priorizar el presente
            log.warning("[%s] backlog de %.0fs, descartando", sid, len(room.buf) / SR)
            room.buf = room.buf[-5 * SR:]
        n = len(room.buf)
        audio = room.buf[:n]
        speech = await asyncio.to_thread(get_speech_timestamps, audio, VAD) if n >= SR // 2 else []
        action, x = decide(speech, n) if n >= SR // 2 else ("wait", 0)
        if not room.alive and action != "final":  # desconectó: vaciar lo que quede
            if not speech:
                return
            action, x = "final", n

        if action == "drop":
            room.buf = room.buf[x:]
        elif action == "partial":
            text, *_ = await asyncio.to_thread(transcribe, audio, room.lang)
            if text and text != room.last_partial:
                room.last_partial = text
                await emit(sid, type="partial", text=text)
        elif action == "final":
            t0 = time.time()
            text, lang, prob = await asyncio.to_thread(transcribe, audio[:x], room.lang)
            room.buf = room.buf[x:]  # solo el processor recorta desde el frente
            room.last_partial = ""
            if not text:
                continue
            # ponytail: se fija el idioma de la sala con la 1ra detección confiable; si el orador cambia de idioma, pasar ?lang=
            if room.lang is None and prob > 0.8:
                room.lang = lang
                log.info("[%s] idioma detectado: %s", sid, lang)
            seg = f"{int(t0 * 1000)}"
            await emit(sid, type="final", seg=seg, text=text, lang=lang, ms=int((time.time() - t0) * 1000))
            t = asyncio.create_task(translate(sid, seg, text, lang, t0))
            bg.add(t)
            t.add_done_callback(bg.discard)


@app.websocket("/ingest/{sid}")
async def ingest(ws: WebSocket, sid: str, lang: str | None = None):
    """Audio PCM s16le mono 16 kHz en frames binarios."""
    if not SID_RE.fullmatch(sid):
        return await ws.close(code=1008)
    await ws.accept()
    if old := rooms.get(sid):  # reconexión: la nueva conexión reemplaza a la vieja
        old.alive = False
        try:
            await old.ws.close()
        except Exception:
            pass
    room = rooms[sid] = Room(sid, lang, ws)
    await r.sadd("sessions", sid)
    task = asyncio.create_task(process(room))
    bg.add(task)
    task.add_done_callback(bg.discard)
    log.info("[%s] conectado (lang=%s)", sid, lang or "auto")
    try:
        while (m := await ws.receive())["type"] != "websocket.disconnect":
            if b := m.get("bytes"):
                room.buf = np.concatenate([room.buf, np.frombuffer(b, np.int16).astype(np.float32) / 32768])
    finally:
        room.alive = False
        if rooms.get(sid) is room:
            del rooms[sid]
        log.info("[%s] desconectado", sid)


@app.get("/mic/{sid}")
async def mic(sid: str):
    return FileResponse(STATIC / "mic.html")


@app.get("/health")
async def health():
    await r.ping()
    return {"ok": True, "rooms": {s: round(len(x.buf) / SR, 1) for s, x in rooms.items()}}
