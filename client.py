import concurrent.futures
import socket
import time
from multiprocessing import Semaphore
from threading import Thread

from constants import *
from protocol import SET, encode_message


def send_request(sock):
    sock.sendall(encode_message(SET, key="alpha", value="beta"))
    data = sock.recv(100)
    print(data)


n = 0


def counter():
    global n
    while True:
        time.sleep(1)
        print(f"throughput is {n} req/s")
        n = 0


if __name__ == "__main__":
    Thread(target=counter).start()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    sock.connect((HOST, PORT))
    while True:
        send_request(sock)
        n += 1
