# Distributed Sort

Système de tri distribué avec plusieurs workers, communication TCP, fusion optimisée et monitoring temps réel.

---

## Réalisé par

- **BAHRI ILHAME**
- **ZAAFA KHADIJA**
- **ALDIEBES GHANEM ISRAA**

### Cadre du projet

Ce projet a été réalisé dans le cadre du module **Système Distribué et Programmation Parallèle** du **Master Intelligence Artificielle**.

Nous allons présenter ce projet à nos camarades de classe du **Master IA**.  
Ils pourront suivre les instructions détaillées dans ce document.

---

# Fonctionnalités principales

- Tri distribué sur plusieurs workers
- Communication réseau via sockets TCP
- Partitionnement intelligent avec échantillonnage de pivots
- Fusion finale optimisée (k-way merge avec min-heap)
- Monitoring temps réel des performances via dashboard web
- Dashboard interactif (Server-Sent Events) avec visualisation live des phases et workers
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
├── monitor.py           # Métriques temps réel (terminal)
├── dashboard.py         # Dashboard web temps réel (Flask + SSE)
├── generate_data.py     # Génère les données de test
├── run_cluster.py       # Lance tout le cluster + dashboard automatiquement
└── config.py            # Paramètres centralisés
```

---

## Instructions pour les étudiants

### 1. Cloner le projet

```bash
git clone https://github.com/Bahriilhame/distributed_sort.git
cd distributed_sort
```

### 2. Installer les dépendances

```bash
pip install rich      # Affichage terminal amélioré (monitoring)
pip install psutil    # Métriques CPU/RAM des workers
pip install flask     # Serveur dashboard web
```

---

# 3. Comment lancer le projet

## Générer les données (1 million d'entiers aléatoires)

```bash
python generate_data.py random 1000000
```

## Lancer le cluster complet

```bash
python run_cluster.py
```

Le script lance automatiquement :
1. Le serveur dashboard sur **http://localhost:5000**
2. Le navigateur web pointant vers le dashboard
3. Les workers TCP
4. Le tri distribué — **uniquement après** que le dashboard soit chargé dans le navigateur

> **Important :** attendez que le terminal affiche `✓ Dashboard prêt — lancement du cluster` avant que le tri commence. Le script synchronise automatiquement le démarrage avec l'ouverture du navigateur.

Une fois le tri terminé, le dashboard reste accessible. Appuyez sur **Ctrl+C** pour quitter.

---

# 4. Tester différents modes

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

# 5. Dashboard web

Le dashboard s'ouvre automatiquement dans le navigateur à l'adresse **http://localhost:5000**.

Il affiche en temps réel :

- **Statut du cluster** (IDLE / RUNNING / DONE)
- **Pipeline d'exécution** — progression phase par phase (Partition → Échantillonnage → Repartitionnement → Tri → Fusion)
- **État de chaque worker** — éléments triés, temps, RAM utilisée
- **Distribution des partitions** — graphique en barres après l'échantillonnage
- **Chronomètre live** et throughput final (éléments/seconde)
- **Journal des événements** horodaté

---

# 6. Résultat attendu dans le terminal

```text
10:42:01  INFO    Dashboard lancé : http://localhost:5000
10:42:01  INFO    En attente que le dashboard soit chargé dans le browser...
10:42:03  INFO    ✓ Dashboard prêt — lancement du cluster
10:42:03  INFO    Démarrage worker 0 sur port 12000 (PID ...)
...
10:42:05  INFO    Vérification des workers...
10:42:05  INFO      ✓ Worker 0 (port 12000) OK
10:42:05  INFO      ✓ Worker 1 (port 12001) OK
...

10:42:05  INFO    Pivots calculés en 0.041s :
                   [199823, 399711, 599450, 799203]

10:42:05  INFO    Partitions après pivots :
                   [200014, 199987, 199823, 200112, 200064]

10:42:06  INFO      Worker 0: 200,014 éléments en 0.031s — 18,432 KB
10:42:06  INFO      Worker 2: 199,823 éléments en 0.028s — 17,890 KB
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