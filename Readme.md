# Distributed Sort

Système de tri distribué avec plusieurs workers, communication TCP, fusion optimisée et monitoring temps réel.

# Fonctionnalités principales

- Tri distribué sur plusieurs workers
- Communication réseau via sockets TCP
- Partitionnement intelligent avec échantillonnage de pivots
- Fusion finale optimisée (k-way merge avec min-heap)
- Monitoring temps réel des performances
- Support de plusieurs types de datasets
- Paramétrage facile du nombre de workers et de la taille des données

## Structure du projet

```bash
distributed_sort/
├── coordinator.py       # Découpe les données, dispatche, fusionne
├── worker.py            # Reçoit une partition, trie, renvoie
├── merger.py            # Fusion k-way avec heap min
├── network.py           # Couche de communication (sockets TCP)
├── sampler.py           # Échantillonnage des pivots (optimisation réseau)
├── monitor.py           # Métriques temps réel
├── generate_data.py     # Génère les données de test
├── run_cluster.py       # Lance tout le cluster localement
└── config.py            # Paramètres centralisés
```

---

# 1. Installation

Installer les dépendances :

```bash
pip install rich      # Affichage terminal amélioré (monitoring)
pip install psutil    # Métriques CPU/RAM des workers
```

---

# 2. Comment lancer le projet

## Générer les données (1 million d'entiers aléatoires)

```bash
python generate_data.py random 1000000
```

## Lancer le cluster complet

```bash
python run_cluster.py
```

---

# 3. Tester différents modes

## Données presque triées (cas difficile)

```bash
python generate_data.py nearly_sorted 500000
python run_cluster.py
```

## Données triées à l'envers

```bash
python generate_data.py reverse 2000000
python run_cluster.py
```

## Changer le nombre de workers

```bash
NUM_WORKERS=8 python run_cluster.py
```

## Changer la taille des données

```bash
DATA_SIZE=5000000 python generate_data.py
python run_cluster.py
```

---

# 4. Résultat attendu dans le terminal

```text
10:42:01  INFO    Vérification des workers...
10:42:01  INFO      ✓ Worker 0 (port 9000) OK
10:42:01  INFO      ✓ Worker 1 (port 9001) OK
...

10:42:01  INFO    Pivots calculés en 0.041s :
                   [199823, 399711, 599450, 799203]

10:42:01  INFO    Partitions après pivots :
                   [200014, 199987, 199823, 200112, 200064]

10:42:02  INFO      Worker 0: 200,014 éléments en 0.031s — 18,432 KB
10:42:02  INFO      Worker 2: 199,823 éléments en 0.028s — 17,890 KB
...
```

### Tableau récapitulatif

```text
╭──────────────────────────────────────────────────╮
│             Résultats du cluster                 │
├────────┬────────────────┬──────────────┬─────────┤
│ Worker │ Éléments triés │ Temps tri(s) │ RAM     │
├────────┼────────────────┼──────────────┼─────────┤
│   0    │    200,014     │    0.0312    │ 18,432  │
│   1    │    199,987     │    0.0298    │ 17,901  │
...
╰──────────────────────────────────────────────────╯

⏱ Temps total : 0.847s
✓ Vérification OK — [12, 34, 45, ...] ... [9999940, 9999967]
```

---
