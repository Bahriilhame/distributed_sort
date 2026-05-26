# worker.py
import socket
import threading
import time
import logging
import psutil
import os
from network import send_data, recv_data
from config import HOST

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, worker_id: int, port: int):
        self.worker_id   = worker_id
        self.port        = port
        self.metrics     = {"sort_time": 0.0, "data_size": 0, "status": "idle"}
        self._lock       = threading.Lock()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, self.port))
        server.listen(10)
        log.info(f"[Worker {self.worker_id}] Écoute sur {HOST}:{self.port}")

        while True:
            conn, addr = server.accept()
            thread = threading.Thread(
                target=self._handle, args=(conn,), daemon=True
            )
            thread.start()

    def _handle(self, conn: socket.socket):
        try:
            msg = recv_data(conn)

            if msg["cmd"] == "SAMPLE_REQUEST":
                # Phase 1 : envoyer un échantillon pour les pivots
                data   = msg["data"]
                from sampler import sample
                s      = sample(data)
                send_data(conn, {"status": "ok", "sample": s})

            elif msg["cmd"] == "SORT":
                # Phase 2 : trier la partition assignée
                data = msg["data"]
                t0   = time.perf_counter()
                data.sort()                        # Timsort natif Python (O(n log n))
                elapsed = time.perf_counter() - t0

                with self._lock:
                    self.metrics["sort_time"] = elapsed
                    self.metrics["data_size"] = len(data)
                    self.metrics["status"]    = "done"

                mem = psutil.Process(os.getpid()).memory_info().rss // 1024
                send_data(conn, {
                    "status":    "ok",
                    "sorted":    data,
                    "sort_time": elapsed,
                    "memory_kb": mem,
                    "worker_id": self.worker_id,
                })
                log.info(
                    f"[Worker {self.worker_id}] {len(data):,} éléments triés "
                    f"en {elapsed:.3f}s ({mem:,} KB RAM)"
                )

            elif msg["cmd"] == "PING":
                send_data(conn, {"status": "pong", "worker_id": self.worker_id})

        except Exception as e:
            log.error(f"[Worker {self.worker_id}] Erreur: {e}")
            try:
                send_data(conn, {"status": "error", "msg": str(e)})
            except Exception:
                pass
        finally:
            conn.close()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    worker_id = int(sys.argv[1])
    port      = int(sys.argv[2])
    w = Worker(worker_id, port)
    w.start()