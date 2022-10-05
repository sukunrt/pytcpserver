import random


def random_bytes(n: int) -> bytes:
    return random_string(n).encode("utf-8")


def random_string(n: int) -> str:
    return "".join([random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(n)])
