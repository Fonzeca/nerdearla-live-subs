"""Manda audio de cualquier fuente que lea ffmpeg a una sala.

  python tools/push.py charla.mp4 ws://localhost:8001/ingest/sala-1
  python tools/push.py srt://0.0.0.0:9000?mode=listener ws://localhost:8001/ingest/sala-2?lang=en
  python tools/push.py "audio=Micrófono (USB)" ws://... --dshow    # dispositivo en Windows
  yt-dlp -o - URL | python tools/push.py - ws://localhost:8001/ingest/sala-3

Los archivos se envían a velocidad real (-re) para simular una charla en vivo.
"""
import asyncio
import os
import subprocess
import sys

import websockets

CHUNK = 3200  # 100 ms de PCM s16le 16 kHz mono


async def main(src, url, dshow=False):
    cmd = ["ffmpeg", "-loglevel", "error"]
    if os.path.exists(src) or src == "-":
        cmd += ["-re"]
    if dshow:
        cmd += ["-f", "dshow"]
    cmd += ["-i", "pipe:0" if src == "-" else src, "-f", "s16le", "-ac", "1", "-ar", "16000", "-"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    chunk = b""
    while True:
        try:
            async with websockets.connect(url) as ws:
                print("conectado a", url, file=sys.stderr)
                while True:
                    if chunk:
                        await ws.send(chunk)
                    chunk = await asyncio.to_thread(proc.stdout.read, CHUNK)
                    if not chunk:
                        return
        except (OSError, websockets.ConnectionClosed) as e:
            print("reconectando:", e, file=sys.stderr)
            await asyncio.sleep(1)


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dshow"]
    if len(args) != 2:
        sys.exit(__doc__)
    asyncio.run(main(*args, dshow="--dshow" in sys.argv))
