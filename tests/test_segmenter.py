"""Correr con: python -m tests.test_segmenter"""
from app.segmenter import SR, decide


def s(a, b):
    return {"start": int(a * SR), "end": int(b * SR)}


def test():
    assert decide([], SR // 4) == ("wait", 0)
    assert decide([], 3 * SR) == ("drop", 3 * SR - SR // 2)
    # hablando todavía (la voz llega hasta el final del buffer)
    assert decide([s(0.1, 2.9)], 3 * SR) == ("partial", 0)
    # 1 s de silencio después de la frase -> final con padding
    assert decide([s(0.1, 2.0)], 3 * SR) == ("final", int(2.2 * SR))
    # padding no se pasa del buffer
    assert decide([s(0.1, 2.0)], int(2.65 * SR)) == ("final", int(2.2 * SR))
    # monólogo sin pausas largas: corta al inicio de la frase actual
    assert decide([s(0, 5), s(5.3, 12.4)], int(12.5 * SR)) == ("final", int(5.3 * SR))
    # una sola frase de 12 s: corta todo
    assert decide([s(0.1, 12.4)], int(12.5 * SR)) == ("final", int(12.5 * SR))


if __name__ == "__main__":
    test()
    print("ok")
