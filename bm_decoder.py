import time

import utils
from protocol import StreamingDecoder, encode_message

num_msgs = 100000
msgs = [
    encode_message("SET", utils.random_string(10), utils.random_string(10))
    for _ in range(num_msgs)
]
msg = b"".join(msgs)
decoder = StreamingDecoder()
t1 = time.time()
decoder.add(msg)
print(time.time() - t1)
print(decoder.next())
