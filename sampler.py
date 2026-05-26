# sampler.py
import random
from typing import List
from config import SAMPLE_RATE, NUM_WORKERS


def sample(data: List[int]) -> List[int]:
    """Prend SAMPLE_RATE% des éléments, aléatoirement."""
    k = max(1, int(len(data) * SAMPLE_RATE))
    return random.sample(data, k)


def compute_pivots(samples: List[List[int]], num_workers: int) -> List[int]:
    """
    Fusionne tous les échantillons, les trie, et choisit (num_workers - 1)
    pivots équidistants — garantit des partitions de taille similaire.
    """
    flat = sorted(x for s in samples for x in s)
    n    = len(flat)
    pivots = [
        flat[int(n * i / num_workers)]
        for i in range(1, num_workers)
    ]
    return pivots


def partition_by_pivots(data: List[int], pivots: List[int]) -> List[List[int]]:
    """
    Divise data en len(pivots)+1 buckets selon les pivots.
    Chaque bucket va à un worker différent.
    """
    buckets: List[List[int]] = [[] for _ in range(len(pivots) + 1)]
    for x in data:
        placed = False
        for i, pivot in enumerate(pivots):
            if x < pivot:
                buckets[i].append(x)
                placed = True
                break
        if not placed:
            buckets[-1].append(x)
    return buckets