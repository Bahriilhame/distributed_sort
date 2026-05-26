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

Exemple réel avec **10 000 000 d'éléments** sur **5 workers** :

```text
02:57:23  INFO    Dashboard lancé : http://localhost:5000
02:57:23  INFO    En attente que le dashboard soit chargé dans le browser...
02:57:23  INFO    ✓ Dashboard prêt — lancement du cluster
02:57:23  INFO    Démarrage worker 0 sur port 12000 (PID 22268)
02:57:24  INFO    Démarrage worker 1 sur port 12001 (PID 9924)
02:57:24  INFO    Démarrage worker 2 sur port 12002 (PID 22048)
02:57:24  INFO    Démarrage worker 3 sur port 12003 (PID 14312)
02:57:24  INFO    Démarrage worker 4 sur port 12004 (PID 13880)
02:57:26  INFO    Vérification des workers...
02:57:26  INFO      ✓ Worker 0 (port 12000) OK
02:57:26  INFO      ✓ Worker 1 (port 12001) OK
02:57:26  INFO      ✓ Worker 2 (port 12002) OK
02:57:26  INFO      ✓ Worker 3 (port 12003) OK
02:57:26  INFO      ✓ Worker 4 (port 12004) OK
02:57:26  INFO    Tri de 10,000,000 éléments sur 5 workers
02:57:26  INFO    Chunks initiaux : [2000000, 2000000, 2000000, 2000000, 2000000]
02:57:27  INFO    Pivots calculés en 0.918s : [1989636, 4002879, 6014582, 8024710]
02:57:32  INFO    Partitions après pivots : [1988139, 2012805, 2014662, 2010210, 1974184]
02:57:34  INFO      Worker 1: 2,012,805 éléments en 1.340s — 99,504 KB
02:57:35  INFO      Worker 0: 1,988,139 éléments en 1.418s — 99,872 KB
02:57:35  INFO      Worker 2: 2,014,662 éléments en 1.485s — 99,360 KB
02:57:35  INFO      Worker 3: 2,010,210 éléments en 1.445s — 99,292 KB
02:57:35  INFO      Worker 4: 1,974,184 éléments en 1.476s — 98,044 KB
02:57:35  INFO    Phase tri terminée en 3.318s
02:57:39  INFO    Fusion k-way en 3.458s
02:57:39  INFO    ✓ Terminé — 10,000,000 éléments triés en 12.391s total
```

### Tableau récapitulatif

```text
╭────────┬────────────────┬───────────────┬──────────────╮
│ Worker │ Éléments triés │ Temps tri (s) │ RAM utilisée │
├────────┼────────────────┼───────────────┼──────────────┤
│   0    │      1,988,139 │        1.4184 │    99,872 KB │
│   1    │      2,012,805 │        1.3403 │    99,504 KB │
│   2    │      2,014,662 │        1.4854 │    99,360 KB │
│   3    │      2,010,210 │        1.4451 │    99,292 KB │
│   4    │      1,974,184 │        1.4757 │    98,044 KB │
╰────────┴────────────────┴───────────────┴──────────────╯

⏱ Temps total : 12.391s
✓ Vérification OK — [3, 3, 5, 5, 6]...[9999994, 9999997, 9999997, 10000000, 10000000]
```

---

### Vue générale du cluster en cours d'exécution

![Dashboard Done](screenshots/dashboard_done.png)

---