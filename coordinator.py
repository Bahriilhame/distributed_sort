"""
coordinator.py — Version avec monitoring dashboard intégré.

Changements par rapport à l'original :
  - Import de dashboard (optionnel — fonctionne sans)
  - Appels notify_* aux moments clés du tri
  - Tout le reste est identique
"""

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

# ── Import du dashboard ─────────────────────────────────────────────
try:
    import dashboard as dash
    DASHBOARD_ENABLED = True
    print(">>> DASHBOARD IMPORTÉ ✓")
except ImportError as e:
    DASHBOARD_ENABLED = False
    print(f">>> DASHBOARD NON TROUVÉ : {e}")

def _notify(fn, *args, **kwargs):
    """Appelle une fonction dashboard seulement si activé."""
    if DASHBOARD_ENABLED:
        try:
            fn(*args, **kwargs)
        except Exception:
            pass  # Le dashboard ne doit jamais crasher le tri


class Coordinator:
    def __init__(self):
        self.monitor = Monitor(NUM_WORKERS)

    def ping_workers(self) -> bool:
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
        port = WORKER_PORTS[worker_idx]
        sock = connect_with_retry(HOST, port)
        send_data(sock, {"cmd": "SAMPLE_REQUEST", "data": chunk})
        resp = recv_data(sock)
        sock.close()
        return resp["sample"]

    def _send_sort(self, worker_idx: int, partition: List[int]) -> dict:
        port = WORKER_PORTS[worker_idx]
        sock = connect_with_retry(HOST, port)
        send_data(sock, {"cmd": "SORT", "data": partition})
        resp = recv_data(sock)
        sock.close()
        return resp

    def sort(self, data: List[int]) -> List[int]:
        total_start = time.perf_counter()
        log.info(f"Tri de {len(data):,} éléments sur {NUM_WORKERS} workers")

        # ── Notifier le démarrage ─────────────────────────────────────────
        _notify(dash.notify_start, len(data), NUM_WORKERS)

        # ── Phase 1 : découpage initial ───────────────────────────────────
        _notify(dash.notify_phase, "Partition initiale")
        t0 = time.perf_counter()
        chunk_size = len(data) // NUM_WORKERS
        chunks = [
            data[i * chunk_size: (i + 1) * chunk_size if i < NUM_WORKERS - 1 else len(data)]
            for i in range(NUM_WORKERS)
        ]
        log.info(f"Chunks initiaux : {[len(c) for c in chunks]}")
        _notify(dash.notify_phase, "Partition initiale", time.perf_counter() - t0)

        # ── Phase 2 : échantillonnage ──────────────────────────────────────
        _notify(dash.notify_phase, "Échantillonnage")
        t_sample = time.perf_counter()
        samples  = []
        with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
            futures = {pool.submit(self._send_sample_request, i, chunks[i]): i
                       for i in range(NUM_WORKERS)}
            for fut in as_completed(futures):
                samples.append(fut.result())

        pivots = compute_pivots(samples, NUM_WORKERS)
        sample_dur = time.perf_counter() - t_sample
        log.info(f"Pivots calculés en {sample_dur:.3f}s : {pivots}")
        _notify(dash.notify_phase, "Échantillonnage", sample_dur)

        # ── Phase 3 : repartitionnement ────────────────────────────────────
        _notify(dash.notify_phase, "Repartitionnement")
        t0 = time.perf_counter()
        partitions = partition_by_pivots(data, pivots)
        log.info(f"Partitions après pivots : {[len(p) for p in partitions]}")
        _notify(dash.notify_pivots, pivots, [len(p) for p in partitions])
        _notify(dash.notify_phase, "Repartitionnement", time.perf_counter() - t0)

        # ── Phase 4 : tri parallèle ────────────────────────────────────────
        _notify(dash.notify_phase, "Tri parallèle")
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
                # Notifier chaque worker terminé
                _notify(dash.notify_worker,
                        idx, len(resp["sorted"]),
                        resp["sort_time"], resp["memory_kb"])
                log.info(
                    f"  Worker {idx}: {len(resp['sorted']):,} éléments "
                    f"en {resp['sort_time']:.3f}s — {resp['memory_kb']:,} KB"
                )

        sort_dur = time.perf_counter() - t_sort
        log.info(f"Phase tri terminée en {sort_dur:.3f}s")
        _notify(dash.notify_phase, "Tri parallèle", sort_dur)

        # ── Phase 5 : fusion k-way ─────────────────────────────────────────
        _notify(dash.notify_phase, "Fusion k-way")
        t_merge = time.perf_counter()
        merged  = kway_merge(results)
        merge_dur = time.perf_counter() - t_merge
        log.info(f"Fusion k-way en {merge_dur:.3f}s")
        _notify(dash.notify_phase, "Fusion k-way", merge_dur)

        total = time.perf_counter() - total_start
        log.info(f"✓ Terminé — {len(merged):,} éléments triés en {total:.3f}s total")
        self.monitor.print_summary(total)

        # ── Notifier la fin ───────────────────────────────────────────────
        _notify(dash.notify_phase, "Terminé")
        _notify(dash.notify_done, total)

        return merged