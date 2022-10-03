from __future__ import annotations

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
