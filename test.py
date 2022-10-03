from cmath import exp

from constants import GET, SET
from protocol import decode_message, encode_message


def test_encode_get():
    res = encode_message(GET, "hello")
    expected = (
        b"\x01\x00\x00\x00\x10\x00\x00\x00\x03\x00\x00\x00GET\x05\x00\x00\x00hello"
    )
    assert expected == res


def test_encode_set():
    res = encode_message(SET, "hello", "world")
    expected = b"\x01\x00\x00\x00\x19\x00\x00\x00\x03\x00\x00\x00SET\x05\x00\x00\x00hello\x05\x00\x00\x00world"
    assert expected == res


def test_decode_command_set():
    assert decode_message(encode_message(SET, "abcd", "defg")) == [
        "SET",
        "abcd",
        "defg",
    ]


def test_decode_command_get():
    assert decode_message(encode_message(GET, "abcd")) == ["GET", "abcd"]
