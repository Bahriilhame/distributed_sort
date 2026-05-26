# coordinator.py
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from network  import send_data, recv_data, connect_with_retry
from sampler  import sample, compute_pivots, partition_by_pivots
from merger   import kway_merge
from monitor  import Monitor
from config   import HOST, WORKER_PORTS, NUM_WORKERS

log = logging.getLogger(__name__)


class Coordinator:
    def __init__(self):
        self.monitor = Monitor(NUM_WORKERS)

    def ping_workers(self) -> bool:
        """Vérifie que tous les workers sont prêts."""
        log.info("Vérification des workers...")
        for i, port in enumerate(WORKER_PORTS):
            try:
                sock = connect_with_retry(HOST, port, retries=15)
                send_data(sock, {"cmd": "PING"})
                resp = recv_data(sock)
                sock.close()
                if resp.get("status") != "pong":
                    raise RuntimeError(f"Worker {i} ne répond pas correctement")
                log.info(f"  ✓ Worker {i} (port {port}) OK")
            except Exception as e:
                log.error(f"  ✗ Worker {i} inaccessible: {e}")
                return False
        return True

    def _send_sample_request(self, worker_idx: int, chunk: List[int]) -> List[int]:
        """Demande un échantillon à un worker."""
        port = WORKER_PORTS[worker_idx]
        sock = connect_with_retry(HOST, port)
        send_data(sock, {"cmd": "SAMPLE_REQUEST", "data": chunk})
        resp = recv_data(sock)
        sock.close()
        return resp["sample"]

    def _send_sort(self, worker_idx: int, partition: List[int]) -> dict:
        """Envoie une partition à trier et récupère le résultat."""
        port = WORKER_PORTS[worker_idx]
        sock = connect_with_retry(HOST, port)
        send_data(sock, {"cmd": "SORT", "data": partition})
        resp = recv_data(sock)
        sock.close()
        return resp

    def sort(self, data: List[int]) -> List[int]:
        total_start = time.perf_counter()
        log.info(f"Tri de {len(data):,} éléments sur {NUM_WORKERS} workers")

        # ── Phase 1 : découpage initial brut ──────────────────────────────
        chunk_size = len(data) // NUM_WORKERS
        chunks = [
            data[i * chunk_size: (i + 1) * chunk_size if i < NUM_WORKERS - 1 else len(data)]
            for i in range(NUM_WORKERS)
        ]
        log.info(f"Chunks initiaux : {[len(c) for c in chunks]}")

        # ── Phase 2 : échantillonnage parallèle ────────────────────────────
        t_sample = time.perf_counter()
        samples  = []
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futures = {pool.submit(self._send_sample_request, i, chunks[i]): i
                       for i in range(NUM_WORKERS)}
            for fut in as_completed(futures):
                samples.append(fut.result())

        pivots = compute_pivots(samples, NUM_WORKERS)
        log.info(f"Pivots calculés en {time.perf_counter()-t_sample:.3f}s : {pivots}")

        # ── Phase 3 : repartitionnement par pivots ─────────────────────────
        partitions = partition_by_pivots(data, pivots)
        log.info(f"Partitions après pivots : {[len(p) for p in partitions]}")

        # ── Phase 4 : tri parallèle ────────────────────────────────────────
        t_sort    = time.perf_counter()
        results   = [None] * NUM_WORKERS
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futures = {pool.submit(self._send_sort, i, partitions[i]): i
                       for i in range(NUM_WORKERS)}
            for fut in as_completed(futures):
                idx  = futures[fut]
                resp = fut.result()
                results[idx] = resp["sorted"]
                self.monitor.record(idx, resp)
                log.info(
                    f"  Worker {idx}: {len(resp['sorted']):,} éléments "
                    f"en {resp['sort_time']:.3f}s — {resp['memory_kb']:,} KB"
                )

        log.info(f"Phase tri terminée en {time.perf_counter()-t_sort:.3f}s")

        # ── Phase 5 : fusion k-way ─────────────────────────────────────────
        t_merge = time.perf_counter()
        merged  = kway_merge(results)
        log.info(f"Fusion k-way en {time.perf_counter()-t_merge:.3f}s")

        total = time.perf_counter() - total_start
        log.info(f"✓ Terminé — {len(merged):,} éléments triés en {total:.3f}s total")
        self.monitor.print_summary(total)
        return merged