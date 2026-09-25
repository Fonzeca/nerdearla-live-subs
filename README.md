# nerdearla-live-subs

Subtítulos en vivo, multi-sala, para conferencias. Transcribe en el idioma original y traduce a español en tiempo real. 100% open source y self-hosted: no depende de ninguna API externa.

Hecho para la [Nerdearla Vibeathon 2026](https://nerdearla26.devpost.com/).

## Arquitectura

```
[Mic / OBS / SRT / archivo] ──PCM 16k──▶ worker  /ingest/{sala}
                                           │  Silero VAD + faster-whisper (large-v3-turbo)
                                           │  parciales cada 0.5 s, final al cerrar la frase
                                           ▼
                                   Redis Stream subs:{sala}  ◀── Gemma 3 (Ollama) traduce los finales
                                           │
                                           ▼
                         gateway (N réplicas)  /ws/{sala} ──▶ /s/{sala}  subtítulos / overlay OBS
```

- **Un modelo, varias salas:** el worker carga whisper una sola vez y transcribe salas en paralelo (`WHISPER_WORKERS`).
- **Segmentación por VAD:** mientras alguien habla se muestran parciales; al detectar 0.6 s de silencio (o 12 s sin pausa) la frase se cierra, se publica y se traduce. La traducción corre aparte y no frena la transcripción.
- **Redis Streams:** los espectadores que se reconectan retoman desde el último mensaje (`?last=`), los que llegan tarde ven el historial, y la transcripción completa queda guardada.
- **Gateway sin estado:** escala horizontal detrás de cualquier balanceador. Para más salas, levantar más workers y repartir las salas entre ellos.
- **Tolerante a fallas:** ingest y viewers se reconectan solos; si una sala se reconecta, la conexión nueva reemplaza a la vieja; si la GPU se atrasa más de 30 s se descarta backlog para volver al presente.

## Levantar

Requiere Docker con GPU NVIDIA (probado con RTX 3050 8 GB: whisper-turbo ~1.6 GB + gemma3:4b ~3.5 GB).

```bash
cp .env.example .env   # opcional
docker compose up -d --build
```

- `http://localhost:8000` — lista de salas y links
- `http://localhost:8000/s/sala-1` — subtítulos (`?overlay` fondo transparente para OBS, `&show=es|orig|both`, `&lines=3`)
- `http://localhost:8001/mic/sala-1` — transmitir el micrófono de la sala desde el navegador (Chrome/Edge; `?lang=en` para fijar el idioma)
- `/health` en ambos servicios

La primera vez descarga los modelos (~5 GB).

### Mandar audio

Desde el navegador: abrir `/mic/{sala}` en la compu conectada a la consola de sonido. Fuera de `localhost` el navegador exige HTTPS para el micrófono.

Desde cualquier fuente que lea ffmpeg:

```bash
pip install websockets
python tools/push.py charla.mp4 ws://localhost:8001/ingest/sala-1          # archivo, a velocidad real
python tools/push.py "srt://0.0.0.0:9000?mode=listener" ws://localhost:8001/ingest/sala-2
python tools/push.py "audio=Micrófono (USB)" ws://localhost:8001/ingest/sala-3 --dshow
yt-dlp -o - https://youtu.be/... | python tools/push.py - ws://localhost:8001/ingest/sala-4
```

Protocolo de ingesta: WebSocket con frames binarios PCM s16le mono 16 kHz. Cualquier cliente que mande eso sirve.

### Sin Docker / sin GPU

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-worker.txt
docker run -d -p 6379:6379 redis:7-alpine && ollama pull gemma3:4b
WHISPER_MODEL=small WHISPER_DEVICE=cpu WHISPER_COMPUTE=int8 uvicorn app.worker:app --port 8001
uvicorn app.gateway:app --port 8000
```

## Configuración

| Variable | Default | |
|---|---|---|
| `WHISPER_MODEL` | `large-v3-turbo` | `small`/`medium` si falta VRAM |
| `WHISPER_DEVICE` / `WHISPER_COMPUTE` | `cuda` / `float16` | `cpu` / `int8` sin GPU |
| `WHISPER_WORKERS` | `2` | transcripciones en paralelo (~1 por sala) |
| `TRANSLATE_MODEL` | `gemma3:4b` | cualquier modelo de Ollama |
| `TARGET_LANG` | `es` | |
| `TICK_S` | `0.5` | cada cuánto se actualizan los parciales |

## Mensajes

Cada entrada del stream (y cada mensaje del WebSocket del viewer) es JSON:

```json
{"id": "...", "type": "partial", "text": "Welcome to Nerd", "ts": 1790000000.1}
{"id": "...", "type": "final", "seg": "1790000000500", "text": "Welcome to Nerdearla.", "lang": "en", "ms": 310}
{"id": "...", "type": "tr", "seg": "1790000000500", "text": "Bienvenidos a Nerdearla.", "ms": 900}
```

`ms` = tiempo de procesamiento desde que se cerró la frase.

## Tests

```bash
python -m tests.test_segmenter
```

## Licencia

MIT
