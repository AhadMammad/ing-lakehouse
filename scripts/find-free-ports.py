"""
Find 9 free host ports for a new ing-lakehouse instance.
Outputs a space-separated list: S3 Console SparkUI SparkMaster Kafka Nessie Jupyter Trino Hue
"""
import socket

def find_free_port(base, used):
    p = base
    while True:
        if p not in used:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(('', p))
                s.close()
                used.add(p)
                return p
            except OSError:
                try:
                    s.close()
                except Exception:
                    pass
        p += 1

used = set()
bases = [9000, 9011, 8080, 7077, 9092, 19120, 8888, 8081, 8000]
ports = [find_free_port(b, used) for b in bases]
print(' '.join(map(str, ports)))
