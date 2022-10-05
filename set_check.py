import random
import socket
import time
from collections import deque
from threading import Thread

from constants import *
from protocol import *
from utils import random_string

num_msg = 0


def get_bytes(sock, n):
    items = []
    got = 0
    while got < n:
        b = sock.recv(n - got)
        items.append(b)
        got += len(b)
    return b"".join(items)


def drain_receiver(sock, q):
    global num_msg
    while True:
        if not len(q):
            time.sleep(0.01)
            continue
        want = q.popleft()
        got = get_bytes(sock, len(want))
        if want != got:
            print(num_msg, want, got)
        assert want == got
        num_msg += 1


def counter():
    global num_msg
    while True:
        time.sleep(1)
        print(num_msg, "req/sec")
        num_msg = 0


if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    q = deque()
    Thread(target=drain_receiver, args=(sock, q)).start()
    Thread(target=counter).start()
    while True:
        k = random_string(100)
        v = random_string(100)
        sock.send(encode_message(SET, k, v))
        q.append(encode_message("True"))
        sock.send(encode_message(GET, k))
        q.append(encode_message("True", v))
