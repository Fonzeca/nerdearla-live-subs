"""Decide qué hacer con el buffer de audio de una sala en cada tick.

Lógica pura (sin modelos) para poder testearla sola.
"""

SR = 16000
SILENCE_S = 0.6  # silencio que cierra una frase
MAX_S = 12.0     # corte forzado si alguien habla sin pausas
PAD_S = 0.2      # cola de audio que se deja después de la última palabra


def decide(speech, n, sr=SR):
    """speech: [{'start', 'end'}] en samples, sobre un buffer de n samples.

    Devuelve una tupla (acción, índice):
      ('wait', 0)        poco audio todavía
      ('drop', desde)    solo silencio: descartar buffer[:desde]
      ('partial', 0)     frase en curso: transcribir para mostrar parcial
      ('final', corte)   frase cerrada: transcribir buffer[:corte] y consumirlo
    """
    if not speech:
        return ("drop", n - sr // 2) if n > sr else ("wait", 0)
    last = speech[-1]
    if n - last["end"] >= SILENCE_S * sr:
        return "final", min(n, last["end"] + int(PAD_S * sr))
    if n >= MAX_S * sr:
        # Cortamos donde empieza la frase actual si hay algo antes; si no, todo.
        return "final", last["start"] if last["start"] >= sr else n
    return "partial", 0
