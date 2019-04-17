import time
import socket
import pickle
import struct


class GraphiteClient:

    def __init__(self, host: str, port: int):
        self.sock = socket.socket()
        self.sock.connect((host, port))

    def close(self):
        self.sock.close()

    def batch_send(self, key_value_pairs):
        log_time = int(time.time())
        tuples = ([])
        for each_pair in key_value_pairs:
            each_key = each_pair[0]
            each_value = each_pair[1]
            tuples.append((each_key, (log_time, each_value)))
        self.batch_send_tuples(tuples)

    def batch_send_tuples(self, tuples):
        package = pickle.dumps(tuples, 1)
        size = struct.pack('!L', len(package))
        self.sock.sendall(size)
        self.sock.sendall(package)

