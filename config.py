# config.py
import os

NUM_WORKERS     = int(os.getenv("NUM_WORKERS", 5))
# BASE_PORT       = int(os.getenv("BASE_PORT", 9000))
BASE_PORT = 12000
HOST            = os.getenv("HOST", "127.0.0.1")
DATA_SIZE       = int(os.getenv("DATA_SIZE", 1_000_000))
SAMPLE_RATE     = float(os.getenv("SAMPLE_RATE", 0.01))  # 1% pour pivots
CHUNK_SIZE      = 8192          # octets par envoi réseau
SORT_ALGORITHM  = "quicksort"   # ou "mergesort"
LOG_LEVEL       = "INFO"

WORKER_PORTS = [BASE_PORT + i for i in range(NUM_WORKERS)]