from __future__ import annotations

from base64 import decode
from collections import deque
from typing import Generator, List, Tuple

VERSION = 1
HEADER_LEN = 8


def decode_header(bs: bytes) -> Tuple[bool, int, int]:
    """
    Decode the msg header from the raw bytes. This is useful from reading the message
    from the connection to determing the total length of the message
    Returns
        success: decode success
        version: version of the protocol
        msg_length: length of the remaining message
    """
    if len(bs) < 8:
        return False, -1, -1
    return (
        True,
        _decode_int(bs[:4]),
        _decode_int(bs[4:8]),
    )


def encode_message(*args):
    """
    encode a command to bytes to be sent over the wire.
    the args are dependant on the command used.

    encode_message("SET", "hello", "world")
    encode_message("GET", "hello")
    encode_message("False")
    """
    encoded_args = [_encode_string(x) for x in args]
    msg = b"".join(encoded_args)
    return _encode_header(VERSION, msg) + msg


def decode_message(s: bytes) -> List[str]:
    """
    decode the command and return the command with the arguments as a list.
    The first element of the list is the command name, the rest of the items in the
    list are the arguments required for executing that command.

    decode_message(encode_message("SET", "a", "b")) == ["SET", "a", "b"]
    """
    items = []
    for success, s in _decode_string(s[8:]):
        if not success:
            raise Exception(f"Failed to decode string {s}")
        items.append(s)
    return items


class StreamingDecoder:

    _WAITING_HEADER = 0
    _WAITING_DATA = 1

    def __init__(self):
        self._buf = Buffer()
        self._state = self._WAITING_HEADER
        self._msgs = deque()
        self._required = 0
        self._header = None

    def ready(self):
        return len(self._msgs) > 0

    def next(self):
        return self._msgs.popleft()

    def add(self, s: bytes):
        self._buf.add(s)
        while True:
            if self._state == self._WAITING_HEADER:
                h = self._buf.get(HEADER_LEN)
                if h is None:
                    return
                success, _, msg_len = decode_header(h)
                if not success:
                    raise Exception(f"invalid header {h}")
                self._required = msg_len
                self._state = self._WAITING_DATA
                self._header = h
            elif self._state == self._WAITING_DATA:
                d = self._buf.get(self._required)
                if d is None:
                    return
                msg = []
                for success, s in _decode_string(d):
                    if not success:
                        raise Exception(f"Failed to decode string {s}")
                    msg.append(s)
                self._msgs.append(msg)
                self._state = self._WAITING_HEADER
                self._header = None
                self._required = 0


class Buffer:
    def __init__(self):
        self._buf = deque()
        self._n = 0

    def add(self, s):
        self._buf.append(memoryview(s))
        self._n += len(s)

    def get(self, n):
        if self._n < n:
            return None
        cnt = 0
        items = []
        while cnt < n:
            x = self._buf.popleft()
            nn = len(x)
            if nn + cnt > n:
                items.append(x[: n - cnt])
                self._buf.appendleft(x[n - cnt :])
                break
            else:
                cnt += len(x)
                items.append(x)
        self._n -= n
        return b"".join(items)


def _encode_header(version, msg):
    return _encode_int(version) + _encode_int(len(msg))


def _encode_int(i: int):
    return i.to_bytes(4, byteorder="little")


def _decode_int(b: bytes):
    return int.from_bytes(b, byteorder="little")


def _encode_string(s: str) -> bytes:
    return _encode_int(len(s)) + s.encode("utf-8")


def _decode_string(bs: bytes) -> Generator[Tuple[bool, str], None, None]:
    i = 0
    while i < len(bs):
        if i + 4 >= len(bs):
            yield False, ""
            return
        sz = _decode_int(bs[i : i + 4])
        yield True, bs[i + 4 : i + 4 + sz].decode("utf-8")
        i = i + 4 + sz
