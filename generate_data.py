# generate_data.py
import random
import pickle
import sys
from config import DATA_SIZE


def generate(n: int = DATA_SIZE, mode: str = "random") -> list:
    if mode == "random":
        return [random.randint(0, 10_000_000) for _ in range(n)]
    elif mode == "sorted":
        return list(range(n))
    elif mode == "reverse":
        return list(range(n, 0, -1))
    elif mode == "nearly_sorted":
        data = list(range(n))
        for _ in range(n // 20):   # 5% de perturbations
            i, j = random.sample(range(n), 2)
            data[i], data[j] = data[j], data[i]
        return data
    raise ValueError(f"Mode inconnu: {mode}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "random"
    n    = int(sys.argv[2]) if len(sys.argv) > 2 else DATA_SIZE
    data = generate(n, mode)
    with open("data.pkl", "wb") as f:
        pickle.dump(data, f)
    print(f"Généré {n:,} éléments (mode={mode}) → data.pkl")