# run_cluster.py
import subprocess
import sys
import time
import pickle
import logging
import threading
import webbrowser

from coordinator import Coordinator
from config      import NUM_WORKERS, WORKER_PORTS
import dashboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# Flag partagé : mis à True quand le JS appelle /api/ready
_browser_ready = threading.Event()


def _patch_dashboard():
    """Ajoute une route /api/ready au dashboard Flask."""
    from flask import Response

    @dashboard.app.route("/api/ready")
    def api_ready():
        _browser_ready.set()
        return Response("ok", mimetype="text/plain")


def main():
    # ── Patcher le dashboard avec /api/ready ───────────────────────────
    _patch_dashboard()

    # ── Démarrer Flask ────────────────────────────────────────────────
    threading.Thread(
        target=dashboard.app.run,
        kwargs={
            "host": "0.0.0.0",
            "port": 5000,
            "debug": False,
            "threaded": True,
            "use_reloader": False,
        },
        daemon=True,
    ).start()

    # ── Démarrer le broadcast SSE ──────────────────────────────────────
    threading.Thread(target=dashboard._broadcast_loop, daemon=True).start()

    # ── Attendre que Flask soit prêt à accepter des connexions ────────
    import socket as _socket
    for _ in range(50):
        try:
            s = _socket.create_connection(("127.0.0.1", 5000), timeout=0.2)
            s.close()
            break
        except OSError:
            time.sleep(0.1)

    # ── Ouvrir le browser ────────────────────────────────────────────
    webbrowser.open("http://localhost:5000")
    log.info("Dashboard lancé : http://localhost:5000")

    # ── Attendre que le JS signale qu'il est prêt (max 30s) ───────────
    log.info("En attente que le dashboard soit chargé dans le browser...")
    if _browser_ready.wait(timeout=30):
        log.info("✓ Dashboard prêt — lancement du cluster")
    else:
        log.warning("Dashboard non confirmé après 30s — lancement quand même")

    time.sleep(0.3)  # petit délai pour que le SSE s'établisse

    # ── Démarrer les workers ───────────────────────────────────────────
    procs = []
    for i, port in enumerate(WORKER_PORTS):
        cmd = [sys.executable, "worker.py", str(i), str(port)]
        p = subprocess.Popen(cmd)
        procs.append(p)
        log.info(f"Démarrage worker {i} sur port {port} (PID {p.pid})")

    time.sleep(2.0)  # laisser les workers bind leurs sockets

    try:
        # ── Charger les données ────────────────────────────────────────
        with open("data.pkl", "rb") as f:
            data = pickle.load(f)

        coord = Coordinator()
        if not coord.ping_workers():
            log.error("Certains workers ne répondent pas. Abandon.")
            sys.exit(1)

        # ── Trier ──────────────────────────────────────────────────────
        result = coord.sort(data)

        # ── Vérification ───────────────────────────────────────────────
        assert result == sorted(data), "ERREUR : résultat incorrect !"
        log.info(f"✓ Vérification OK — {result[:5]}...{result[-5:]}")

        with open("result.pkl", "wb") as f:
            pickle.dump(result, f)
        log.info("Résultat sauvegardé dans result.pkl")

        # ── Garder le dashboard vivant après la fin ────────────────────
        log.info("Tri terminé. Dashboard disponible — Ctrl+C pour quitter.")
        while True:
            time.sleep(1)

    finally:
        for p in procs:
            p.terminate()
        log.info("Workers arrêtés.")


if __name__ == "__main__":
    main()