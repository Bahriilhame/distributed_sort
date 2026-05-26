# run_cluster.py
import subprocess
import sys
import time
import pickle
import logging

from coordinator import Coordinator
from config      import NUM_WORKERS, WORKER_PORTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


def main():
    # ── Démarrer les workers en sous-processus ──────────────────────────
    procs = []
    for i, port in enumerate(WORKER_PORTS):
        cmd = [sys.executable, "worker.py", str(i), str(port)]
        p   = subprocess.Popen(cmd)
        procs.append(p)
        log.info(f"Démarrage worker {i} sur port {port} (PID {p.pid})")

    time.sleep(3.0)   # Laisser les workers bind leurs sockets

    try:
        # ── Charger les données ─────────────────────────────────────────
        with open("data.pkl", "rb") as f:
            data = pickle.load(f)

        coord = Coordinator()
        if not coord.ping_workers():
            log.error("Certains workers ne répondent pas. Abandon.")
            sys.exit(1)

        # ── Trier ───────────────────────────────────────────────────────
        result = coord.sort(data)

        # ── Vérification ────────────────────────────────────────────────
        assert result == sorted(data), "ERREUR : résultat incorrect !"
        log.info(f"✓ Vérification OK — {result[:5]}...{result[-5:]}")

        with open("result.pkl", "wb") as f:
            pickle.dump(result, f)
        log.info("Résultat sauvegardé dans result.pkl")

    finally:
        for p in procs:
            p.terminate()
        log.info("Workers arrêtés.")


if __name__ == "__main__":
    main()