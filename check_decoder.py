import random
import time

from protocol import StreamingDecoder, decode_message, encode_message


def get_random_string(n):
    return "".join([random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(n)])


t = time.time()
M = 5000
dec = StreamingDecoder()
msgs = []
for i in range(M):
    msg = encode_message(get_random_string(random.randint(10, 300)))
    dec.add(msg)
    msgs.append(dec.next())
el = time.time() - t
print(M / el, "msg / s")


t = time.time()
M = 5000
msgs = []
for i in range(M):
    msg = encode_message(get_random_string(random.randint(10, 300)))
    msgs.append(decode_message(msg))
el = time.time() - t
print(M / el, "msg / s")
