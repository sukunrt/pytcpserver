import random
import select
import socket
import time
from collections import deque

from constants import *
from protocol import StreamingDecoder, encode_message
from store import KVServer


class KQServer:

    """
    A single threaded kqueue server that serves multiple clients with pipelining etc
    """

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._kq = None
        self._sock = None
        self._write_capacity = {}
        self._write_q = {}
        self._conn_map = {}
        self._readers = {}

    def serve(self, func):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._host, self._port))
        sock.listen(5)
        sock.setblocking(0)
        self._sock = sock
        self._kq = select.kqueue()
        self._kq.control(
            [select.kevent(sock, select.KQ_FILTER_READ, select.KQ_EV_ADD)], 0
        )
        print(f"server started on {(HOST, PORT)}")
        while True:
            events = self._kq.control(None, 1000, 10)
            if not events:
                print("no events to report")
            else:
                for event in events:
                    if event.ident == self._sock.fileno():
                        conn, _ = self._sock.accept()
                        conn.setblocking(0)
                        self._register(conn)
                        self._conn_map[conn.fileno()] = conn
                        self._readers[conn.fileno()] = StreamingDecoder()
                        self._write_q[conn.fileno()] = deque()
                        self._write_capacity[conn.fileno()] = 0
                    elif (
                        event.filter == select.KQ_FILTER_READ
                        and event.flags & select.KQ_EV_EOF != 0
                    ):
                        conn = self._conn_map[event.ident]
                        self._unregister(conn)
                        conn.close()
                        self._write_capacity.pop(conn.fileno(), None)
                        self._write_q.pop(conn.fileno(), None)
                        self._conn_map.pop(conn.fileno(), None)
                    elif event.filter == select.KQ_FILTER_READ:
                        conn = self._conn_map[event.ident]
                        data = conn.recv(event.data)
                        reader = self._readers[event.ident]
                        reader.add(data)
                        while reader.ready():
                            msg = reader.next()
                            response = func(msg)
                            self._write_q[event.ident].append(encode_message(*response))
                    elif event.filter == select.KQ_FILTER_WRITE:
                        self._write_capacity[event.ident] = event.data
                        self.flush_writes(event.ident)

    def _register(self, conn):
        self._kq.control(
            [
                select.kevent(conn, select.KQ_FILTER_READ, select.KQ_EV_ADD),
                select.kevent(conn, select.KQ_FILTER_WRITE, select.KQ_EV_ADD),
            ],
            0,
        )

    def _unregister(self, conn):
        self._kq.control(
            [
                select.kevent(conn, select.KQ_FILTER_READ, select.KQ_EV_DELETE),
                select.kevent(conn, select.KQ_FILTER_WRITE, select.KQ_EV_DELETE),
            ],
            0,
        )

    def flush_writes(self, fileno):
        q = self._write_q[fileno]
        n = self._write_capacity[fileno]
        if q:
            item = q.popleft()
            if len(item) > n:
                q.appendleft(item[n:])
                self._conn_map[fileno].sendall(item[:n])
            else:
                self._conn_map[fileno].sendall(item)


if __name__ == "__main__":
    kv_server = KVServer()
    server = KQServer(HOST, PORT)
    server.serve(kv_server.handle)
