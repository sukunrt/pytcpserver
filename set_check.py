import random
import socket
import time
from collections import deque
from threading import Thread

from constants import *
from protocol import *


def get_random_string(n):
    return "".join([random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(n)])


num_msgs = 0


def receive_response(sock):
    data = b""
    while True:
        res = sock.recv(1024)
        data = data + res
        while True:
            success, _, msg_len = decode_header(data)
            if success:
                remaining = msg_len - len(data) + HEADER_LEN
                while remaining > 0:
                    res = sock.recv(remaining)
                    remaining -= len(res)
                    data += res

                if remaining < 0:
                    msg = data[: HEADER_LEN + msg_len]
                    data = data[HEADER_LEN + msg_len :]
                else:
                    msg = data
                    data = b""
                resp = decode_message(msg)
                yield resp
            else:
                break


def drain_receiver(receiver, q):
    global num_msgs
    while True:
        try:
            expected = q.popleft()
        except IndexError:
            continue
        received = next(receiver)
        num_msgs += 1


def counter():
    global num_msgs
    while True:
        time.sleep(1)
        print(num_msgs, "req/sec")
        num_msgs = 0


if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    expected_responses = []
    receiver = receive_response(sock)
    q = deque()
    Thread(target=drain_receiver, args=(receiver, q)).start()
    Thread(target=counter).start()
    while True:
        k = get_random_string(random.randint(10, 100))
        v = get_random_string(random.randint(30, 300))
        sock.send(encode_message(SET, k, v))
        sock.send(encode_message(GET, k))
        q.append(["True"])
        q.append(["True", v])
