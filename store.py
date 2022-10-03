class Store:
    def __init__(self):
        self._db = {}

    def set(self, key, value):
        self._db[key] = value

    def get(self, key):
        return self._db.get(key)


class KVServer:
    def __init__(self):
        self._store = Store()

    def handle(self, command):
        if command[0] == "GET":
            res = self._store.get(command[1])
            if res is None:
                return ["False"]
            else:
                return ["True", res]
        elif command[0] == "SET":
            self._store.set(command[1], command[2])
            return ["True"]
