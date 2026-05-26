# network.py
import socket
import pickle
import struct
import time
import logging

log = logging.getLogger(__name__)


def send_data(sock: socket.socket, data: object) -> None:
    """Sérialise et envoie un objet avec un en-tête de longueur (4 octets)."""
    payload = pickle.dumps(data)
    header  = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


def recv_data(sock: socket.socket) -> object:
    """Reçoit et désérialise un objet (bloquant)."""
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        raise ConnectionError("Connexion fermée par le pair")
    length  = struct.unpack(">I", raw_len)[0]
    payload = _recv_exact(sock, length)
    return pickle.loads(payload)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Reçoit exactement n octets."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket fermé prématurément")
        buf += chunk
    return buf


def connect_with_retry(host: str, port: int, retries: int = 10, delay: float = 0.5):
    """Connexion TCP avec tentatives (workers peuvent démarrer lentement)."""
    for attempt in range(retries):
        try:
            sock = socket.create_connection((host, port), timeout=5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return sock
        except (ConnectionRefusedError, TimeoutError):
            if attempt < retries - 1:
                log.debug(f"Connexion à {host}:{port} échouée, tentative {attempt+1}/{retries}")
                time.sleep(delay)
    raise ConnectionError(f"Impossible de joindre {host}:{port} après {retries} tentatives")