import socket


class UDPServer:
    def __init__(self, host, port):
        self._host = host
        self._port = port

    def serve(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self._host, self._port))
        while True:
            msg, address = sock.recvfrom(10)
            print(msg.decode("utf-8"), address)


if __name__ == "__main__":
    server = UDPServer("localhost", 7010)
    server.serve()
