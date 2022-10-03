import socket
import time
from collections import deque
from multiprocessing import Semaphore
from threading import Thread, current_thread

from constants import *
from protocol import *
from store import KVServer, Store


class Server:
    def __init__(self, host, port):
        self.addr = (host, port)
        self.host = host
        self.port = port
        self._store = Store()

    def serve(self, func):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.addr)
        sock.listen(10)
        print(f"server started on {sock.getsockname()}")
        while True:
            conn, _ = sock.accept()
            conn = Connection(conn, func)
            conn.start_reader()
            conn.start_writer()


class Connection:
    def __init__(self, conn: socket.socket, func):
        self._conn = conn
        self._read_buf = b""
        self._q = deque()
        self._func = func
        self._reader_thread = None
        self._writer_thread = None
        self._closed = False

    def read_cmd(self):
        if self._closed:
            return True
        try:
            success, _, msg_len = decode_header(self._read_buf)
            while not success:
                res = self._conn.recv(1024)
                if not res:
                    self.close()
                    break
                self._read_buf += res
                success, _, msg_len = decode_header(self._read_buf)
            remaining = msg_len - len(self._read_buf) + HEADER_LEN
            while remaining > 0:
                res = self._conn.recv(remaining)
                if not res:
                    self.close()
                    break
                remaining -= len(res)
                self._read_buf += res
            if remaining < 0:
                msg = self._read_buf[: HEADER_LEN + msg_len]
                self._read_buf = self._read_buf[HEADER_LEN + msg_len :]
            else:
                msg = self._read_buf
                self._read_buf = b""
            self._q.append(decode_message(msg))
        except (ConnectionError, OSError):
            self.close()

    def process_command(self):
        try:
            cmd = self._q.popleft()
        except IndexError:
            time.sleep(0.01)
            return
        response = self._func(cmd)
        try:
            self._conn.send(encode_message(*response))
        except (ConnectionError, OSError):
            self._closed = True
            self._conn.close()

    def start_reader(self):
        if self._reader_thread is not None:
            return

        def reader():
            while True:
                if self._closed:
                    print("closing reader")
                    return
                self.read_cmd()

        self._reader_thread = Thread(target=reader)
        self._reader_thread.start()

    def start_writer(self):
        if self._writer_thread is not None:
            return

        def writer():
            while True:
                if self._closed:
                    print("closing writer")
                    return
                self.process_command()

        self._writer_thread = Thread(target=writer)
        self._writer_thread.start()

    def close(self):
        self._closed = True
        self._conn.close()


if __name__ == "__main__":
    server = Server(HOST, PORT)
    kvserver = KVServer()
    server.serve(kvserver.handle)
