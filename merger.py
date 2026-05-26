# merger.py
import heapq
from typing import List, Iterator


def kway_merge(sorted_lists: List[List[int]]) -> List[int]:
    """
    Fusion k listes triées en O(N log k) avec un heap min.
    Plus efficace que fusionner séquentiellement (O(N*k)).
    """
    result  = []
    heap    = []

    # Initialiser le heap : (valeur, index_liste, index_dans_liste)
    iterators = [iter(lst) for lst in sorted_lists]
    for i, it in enumerate(iterators):
        try:
            val = next(it)
            heapq.heappush(heap, (val, i, it))
        except StopIteration:
            pass

    while heap:
        val, i, it = heapq.heappop(heap)
        result.append(val)
        try:
            next_val = next(it)
            heapq.heappush(heap, (next_val, i, it))
        except StopIteration:
            pass

    return result


def streaming_kway_merge(sorted_lists: List[List[int]]) -> Iterator[int]:
    """
    Version générateur — utile si le résultat ne tient pas en RAM.
    Produit les éléments un par un sans stocker le résultat complet.
    """
    heap = []
    iterators = [iter(lst) for lst in sorted_lists]
    for i, it in enumerate(iterators):
        try:
            val = next(it)
            heapq.heappush(heap, (val, i, it))
        except StopIteration:
            pass

    while heap:
        val, i, it = heapq.heappop(heap)
        yield val
        try:
            next_val = next(it)
            heapq.heappush(heap, (next_val, i, it))
        except StopIteration:
            pass