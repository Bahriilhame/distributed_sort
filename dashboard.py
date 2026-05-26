"""
dashboard.py — Interface web de monitoring du cluster de tri distribué
Utilise Flask + SSE (Server-Sent Events) — aucune dépendance extra.

Usage :
    1. Lancer le dashboard : python dashboard.py
    2. Ouvrir http://localhost:5000 dans le navigateur
    3. Lancer le tri    : python run_cluster.py (dans un autre terminal)
"""

import json
import queue
import threading
import time
from flask import Flask, Response, render_template_string

# ── Import du coordinateur pour récupérer les métriques ──────────────────────
# On injecte un "observer" dans le coordinator via un singleton partagé

app = Flask(__name__)

# File d'événements partagée entre coordinator et le serveur SSE
event_queue: queue.Queue = queue.Queue(maxsize=200)

# État global du cluster (mis à jour par le coordinateur)
cluster_state = {
    "status": "idle",           # idle | running | done | error
    "phase": "",
    "phase_idx": 0,             # 0-5
    "total_elements": 0,
    "num_workers": 0,
    "start_time": None,
    "end_time": None,
    "workers": {},              # {id: {elements, sort_time, memory_kb, status}}
    "phases_timing": {},        # {phase_name: duration_s}
    "pivots": [],
    "partition_sizes": [],
    "total_time": None,
}
state_lock = threading.Lock()

PHASES = [
    "Partition initiale",
    "Échantillonnage",
    "Repartitionnement",
    "Tri parallèle",
    "Fusion k-way",
    "Terminé",
]


# ── API publique appelée par le coordinateur ──────────────────────────────────

def notify_start(total_elements: int, num_workers: int):
    with state_lock:
        cluster_state.update({
            "status": "running",
            "phase": PHASES[0],
            "phase_idx": 0,
            "total_elements": total_elements,
            "num_workers": num_workers,
            "start_time": time.time(),
            "end_time": None,
            "workers": {i: {"elements": 0, "sort_time": 0,
                            "memory_kb": 0, "status": "waiting"}
                        for i in range(num_workers)},
            "phases_timing": {},
            "pivots": [],
            "partition_sizes": [],
            "total_time": None,
        })
    _push("start", {"total_elements": total_elements, "num_workers": num_workers})


def notify_phase(phase_name: str, duration: float = None):
    idx = PHASES.index(phase_name) if phase_name in PHASES else 0
    with state_lock:
        cluster_state["phase"] = phase_name
        cluster_state["phase_idx"] = idx
        if duration is not None:
            cluster_state["phases_timing"][phase_name] = round(duration, 4)
    _push("phase", {"phase": phase_name, "phase_idx": idx, "duration": duration})


def notify_pivots(pivots: list, partition_sizes: list):
    with state_lock:
        cluster_state["pivots"] = pivots
        cluster_state["partition_sizes"] = partition_sizes
    _push("pivots", {"pivots": pivots, "partition_sizes": partition_sizes})


def notify_worker(worker_id: int, elements: int, sort_time: float, memory_kb: int):
    with state_lock:
        cluster_state["workers"][worker_id] = {
            "elements": elements,
            "sort_time": round(sort_time, 4),
            "memory_kb": memory_kb,
            "status": "done",
        }
    _push("worker_done", {
        "worker_id": worker_id,
        "elements": elements,
        "sort_time": round(sort_time, 4),
        "memory_kb": memory_kb,
    })


def notify_done(total_time: float):
    with state_lock:
        cluster_state["status"] = "done"
        cluster_state["phase"] = "Terminé"
        cluster_state["phase_idx"] = 5
        cluster_state["end_time"] = time.time()
        cluster_state["total_time"] = round(total_time, 4)
    _push("done", {"total_time": round(total_time, 4)})


def notify_error(msg: str):
    with state_lock:
        cluster_state["status"] = "error"
    _push("error", {"msg": msg})


def _push(event: str, data: dict):
    payload = {"event": event, "data": data, "ts": time.time()}
    try:
        event_queue.put_nowait(payload)
    except queue.Full:
        pass  # Drop oldest if queue full


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/state")
def api_state():
    with state_lock:
        return Response(
            json.dumps(cluster_state),
            mimetype="application/json"
        )


@app.route("/api/events")
def sse_stream():
    """Server-Sent Events stream — pousse les événements en temps réel."""
    def generate():
        # Envoyer l'état initial
        with state_lock:
            state_copy = dict(cluster_state)
        yield f"data: {json.dumps({'event': 'state', 'data': state_copy})}\n\n"

        client_q = queue.Queue()
        listeners.add(client_q)
        try:
            while True:
                try:
                    payload = client_q.get(timeout=25)
                    yield f"data: {json.dumps(payload)}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"  # Keep-alive
        except GeneratorExit:
            pass
        finally:
            listeners.discard(client_q)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no"})


# Broadcast à tous les clients SSE connectés
listeners: set = set()


def _broadcast_loop():
    """Distribue les événements SSE à tous les clients."""
    while True:
        try:
            payload = event_queue.get(timeout=1)

            dead = set()

            for q in list(listeners):
                try:
                    q.put_nowait(payload)
                except queue.Full:
                    dead.add(q)

            listeners.difference_update(dead)

        except queue.Empty:
            pass


# ── HTML du dashboard ─────────────────────────────────────────────────────────
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Distributed Sort — Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:      #080e1a;
    --bg2:     #0d1929;
    --bg3:     #112240;
    --border:  #1e3a5f;
    --cyan:    #00c8ff;
    --green:   #00e5aa;
    --amber:   #ffaa00;
    --red:     #ff4d6d;
    --muted:   #5a7a9a;
    --text:    #ccd6f6;
    --white:   #e6f1ff;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Syne', sans-serif;
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* ── Scanline overlay ─────────────────────── */
  body::before {
    content: '';
    position: fixed; inset: 0;
    background: repeating-linear-gradient(
      0deg,
      transparent,
      transparent 2px,
      rgba(0,200,255,0.012) 2px,
      rgba(0,200,255,0.012) 4px
    );
    pointer-events: none;
    z-index: 1000;
  }

  /* ── Header ───────────────────────────────── */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 1.2rem 2rem;
    border-bottom: 1px solid var(--border);
    background: var(--bg2);
    position: sticky; top: 0; z-index: 100;
  }
  .logo {
    display: flex; align-items: center; gap: 0.8rem;
  }
  .logo-icon {
    width: 36px; height: 36px;
    border: 2px solid var(--cyan);
    border-radius: 6px;
    display: flex; align-items: center; justify-content: center;
    font-family: 'JetBrains Mono', monospace;
    font-size: 14px; font-weight: 700;
    color: var(--cyan);
    animation: pulse-border 2s ease-in-out infinite;
  }
  @keyframes pulse-border {
    0%,100% { box-shadow: 0 0 0 0 rgba(0,200,255,0.4); }
    50%      { box-shadow: 0 0 0 6px rgba(0,200,255,0); }
  }
  .logo-text { font-size: 1.1rem; font-weight: 800; color: var(--white); }
  .logo-sub  { font-size: 0.7rem; color: var(--muted);
               font-family: 'JetBrains Mono', monospace; letter-spacing: 2px; }

  #status-badge {
    padding: 0.35rem 1rem;
    border-radius: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem; font-weight: 600;
    letter-spacing: 1px;
    border: 1px solid;
    transition: all 0.4s ease;
  }
  .badge-idle    { color: var(--muted);  border-color: var(--muted);  background: transparent; }
  .badge-running { color: var(--cyan);   border-color: var(--cyan);
                   background: rgba(0,200,255,0.08);
                   animation: blink-badge 1.2s ease-in-out infinite; }
  .badge-done    { color: var(--green);  border-color: var(--green);  background: rgba(0,229,170,0.08); }
  .badge-error   { color: var(--red);    border-color: var(--red);    background: rgba(255,77,109,0.08); }
  @keyframes blink-badge {
    0%,100% { opacity: 1; } 50% { opacity: 0.6; }
  }

  /* ── Main layout ──────────────────────────── */
  main {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    grid-template-rows: auto auto auto;
    gap: 1.2rem;
    padding: 1.5rem 2rem;
    max-width: 1400px;
    margin: 0 auto;
  }

  /* ── Cards ────────────────────────────────── */
  .card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.4rem;
    position: relative;
    overflow: hidden;
    transition: border-color 0.3s;
  }
  .card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--accent, var(--cyan)), transparent);
    opacity: 0.6;
  }
  .card:hover { border-color: rgba(0,200,255,0.3); }
  .card-title {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem; font-weight: 600;
    letter-spacing: 3px; text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 1rem;
  }

  /* ── Stat cards (top row) ─────────────────── */
  .stats-row {
    grid-column: 1 / -1;
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
  }
  .stat-card {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 1.2rem 1.4rem;
    position: relative;
    overflow: hidden;
  }
  .stat-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem; letter-spacing: 2px;
    color: var(--muted); text-transform: uppercase;
    margin-bottom: 0.5rem;
  }
  .stat-value {
    font-size: 2rem; font-weight: 800;
    color: var(--white);
    transition: color 0.5s;
    font-family: 'JetBrains Mono', monospace;
  }
  .stat-sub {
    font-size: 0.75rem; color: var(--muted);
    margin-top: 0.3rem;
    font-family: 'JetBrains Mono', monospace;
  }
  .stat-card .accent-glow {
    position: absolute; right: -20px; top: -20px;
    width: 80px; height: 80px;
    border-radius: 50%;
    opacity: 0.08;
  }

  /* ── Phases pipeline ──────────────────────── */
  .phases-card { grid-column: 1 / 3; --accent: var(--cyan); }
  .phases-track {
    display: flex; align-items: center; gap: 0; margin-top: 0.5rem;
    width: 100%;
  }
  .phase-step {
    flex: 1; text-align: center; position: relative;
    padding: 0.8rem 0.3rem;
    transition: all 0.4s ease;
  }
  .phase-step::after {
    content: '';
    position: absolute; right: 0; top: 50%;
    transform: translateY(-50%);
    width: 1px; height: 60%;
    background: var(--border);
  }
  .phase-step:last-child::after { display: none; }
  .phase-dot {
    width: 28px; height: 28px;
    border-radius: 50%;
    border: 2px solid var(--border);
    display: flex; align-items: center; justify-content: center;
    margin: 0 auto 0.5rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem; font-weight: 700;
    transition: all 0.4s ease;
  }
  .phase-name {
    font-size: 0.65rem;
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    transition: color 0.3s;
    line-height: 1.3;
  }
  .phase-time {
    font-size: 0.6rem; color: var(--green);
    font-family: 'JetBrains Mono', monospace;
    margin-top: 0.2rem; min-height: 0.8rem;
  }
  .phase-step.done .phase-dot {
    background: rgba(0,229,170,0.15);
    border-color: var(--green); color: var(--green);
  }
  .phase-step.done .phase-name { color: var(--green); }
  .phase-step.active .phase-dot {
    background: rgba(0,200,255,0.15);
    border-color: var(--cyan); color: var(--cyan);
    box-shadow: 0 0 12px rgba(0,200,255,0.4);
    animation: spin-dot 1.5s linear infinite;
  }
  .phase-step.active .phase-name { color: var(--cyan); }
  @keyframes spin-dot {
    0%   { box-shadow: 0 0 6px rgba(0,200,255,0.3); }
    50%  { box-shadow: 0 0 16px rgba(0,200,255,0.7); }
    100% { box-shadow: 0 0 6px rgba(0,200,255,0.3); }
  }

  /* ── Total time card ──────────────────────── */
  .time-card { --accent: var(--amber); }
  #total-time-display {
    font-family: 'JetBrains Mono', monospace;
    font-size: 3rem; font-weight: 700;
    color: var(--amber);
    margin: 0.5rem 0;
    text-align: center;
    text-shadow: 0 0 20px rgba(255,170,0,0.3);
  }
  #live-timer {
    text-align: center;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem; color: var(--muted);
  }

  /* ── Workers grid ─────────────────────────── */
  .workers-card { grid-column: 1 / 3; --accent: var(--green); }
  .workers-grid { display: grid; gap: 0.6rem; margin-top: 0.3rem; }
  .worker-row {
    display: grid;
    grid-template-columns: 80px 1fr 90px 90px 80px;
    align-items: center;
    gap: 0.8rem;
    padding: 0.7rem 0.8rem;
    background: var(--bg3);
    border-radius: 8px;
    border: 1px solid var(--border);
    transition: all 0.4s ease;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }
  .worker-row.done  { border-color: rgba(0,229,170,0.3); }
  .worker-row.active { border-color: var(--cyan);
                       box-shadow: 0 0 8px rgba(0,200,255,0.15); }
  .worker-id { color: var(--cyan); font-weight: 700; }
  .worker-bar-wrap {
    background: var(--bg); border-radius: 4px;
    height: 6px; overflow: hidden;
  }
  .worker-bar {
    height: 100%; width: 0%;
    border-radius: 4px;
    transition: width 0.8s ease;
    background: linear-gradient(90deg, var(--green), var(--cyan));
  }
  .worker-row.active .worker-bar {
    animation: shimmer-bar 1s ease-in-out infinite;
  }
  @keyframes shimmer-bar {
    0%,100% { opacity: 0.7; } 50% { opacity: 1; }
  }
  .worker-stat { text-align: right; }
  .worker-stat.time  { color: var(--amber); }
  .worker-stat.ram   { color: var(--muted); }
  .worker-status {
    text-align: center;
    font-size: 0.65rem; letter-spacing: 1px;
  }
  .ws-waiting { color: var(--muted); }
  .ws-sorting { color: var(--cyan); animation: blink-badge 0.8s infinite; }
  .ws-done    { color: var(--green); }

  /* ── Log panel ────────────────────────────── */
  .log-card { --accent: var(--muted); }
  #log-container {
    height: 360px; overflow-y: auto;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.72rem; line-height: 1.8;
    color: var(--text);
  }
  #log-container::-webkit-scrollbar { width: 4px; }
  #log-container::-webkit-scrollbar-track { background: transparent; }
  #log-container::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
  .log-line { padding: 0.05rem 0; border-bottom: 1px solid rgba(30,58,95,0.3); }
  .log-ts { color: var(--muted); margin-right: 0.6rem; }
  .log-ev { margin-right: 0.5rem; }
  .ev-start   { color: var(--cyan); }
  .ev-phase   { color: var(--amber); }
  .ev-worker  { color: var(--green); }
  .ev-done    { color: var(--green); font-weight: 700; }
  .ev-error   { color: var(--red); }
  .ev-pivot   { color: #a78bfa; }

  /* ── Partition chart ──────────────────────── */
  .partitions-card { --accent: #a78bfa; }
  .bar-chart {
    display: flex; align-items: flex-end;
    gap: 8px; height: 140px;
    margin-top: 1rem; padding: 0 0.5rem;
  }
  .bar-col {
    flex: 1; display: flex; flex-direction: column;
    align-items: center; gap: 4px;
  }
  .bar-fill {
    width: 100%; border-radius: 4px 4px 0 0;
    background: linear-gradient(180deg, #a78bfa, #6d28d9);
    transition: height 0.8s ease;
    min-height: 4px;
  }
  .bar-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.6rem; color: var(--muted);
    text-align: center;
  }
  .bar-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.6rem; color: #a78bfa;
    text-align: center;
  }
  .no-data {
    color: var(--muted); font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem; text-align: center;
    margin-top: 2rem; opacity: 0.5;
  }

  /* ── Responsive ───────────────────────────── */
  @media (max-width: 900px) {
    main { grid-template-columns: 1fr; }
    .stats-row { grid-template-columns: repeat(2,1fr); }
    .phases-card, .workers-card { grid-column: 1; }
  }

  /* ── Entrance animations ──────────────────── */
  .card { animation: fade-up 0.5s ease both; }
  .card:nth-child(2) { animation-delay: 0.05s; }
  .card:nth-child(3) { animation-delay: 0.10s; }
  .card:nth-child(4) { animation-delay: 0.15s; }
  .card:nth-child(5) { animation-delay: 0.20s; }
  @keyframes fade-up {
    from { opacity: 0; transform: translateY(16px); }
    to   { opacity: 1; transform: translateY(0); }
  }
</style>
</head>
<body>

<header>
  <div class="logo">
    <div class="logo-icon">DS</div>
    <div>
      <div class="logo-text">Distributed Sort</div>
      <div class="logo-sub">CLUSTER MONITOR</div>
    </div>
  </div>
  <div id="status-badge" class="badge-idle">● IDLE</div>
</header>

<main>

  <!-- Stats row -->
  <div class="stats-row">
    <div class="stat-card">
      <div class="stat-label">Éléments total</div>
      <div class="stat-value" id="s-elements">—</div>
      <div class="stat-sub" id="s-workers-info">En attente du cluster</div>
      <div class="accent-glow" style="background:var(--cyan)"></div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Workers actifs</div>
      <div class="stat-value" id="s-workers">—</div>
      <div class="stat-sub" id="s-workers-done">0 terminés</div>
      <div class="accent-glow" style="background:var(--green)"></div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Phase courante</div>
      <div class="stat-value" style="font-size:1.1rem;line-height:1.4;padding-top:0.4rem" id="s-phase">—</div>
      <div class="stat-sub" id="s-phase-sub"></div>
      <div class="accent-glow" style="background:var(--amber)"></div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Throughput</div>
      <div class="stat-value" id="s-throughput">—</div>
      <div class="stat-sub">éléments / seconde</div>
      <div class="accent-glow" style="background:#a78bfa"></div>
    </div>
  </div>

  <!-- Phases pipeline -->
  <div class="card phases-card">
    <div class="card-title">Pipeline d'exécution</div>
    <div class="phases-track" id="phases-track">
      <!-- filled by JS -->
    </div>
  </div>

  <!-- Total time -->
  <div class="card time-card">
    <div class="card-title">Temps d'exécution</div>
    <div id="total-time-display">0.000s</div>
    <div id="live-timer">En attente...</div>
  </div>

  <!-- Workers -->
  <div class="card workers-card">
    <div class="card-title">État des workers</div>
    <div class="workers-grid" id="workers-grid">
      <div class="no-data">En attente du démarrage du cluster...</div>
    </div>
  </div>

  <!-- Partition chart -->
  <div class="card partitions-card">
    <div class="card-title">Distribution des partitions</div>
    <div class="bar-chart" id="bar-chart">
      <div class="no-data" style="width:100%">Données disponibles après échantillonnage</div>
    </div>
  </div>

  <!-- Log -->
  <div class="card log-card" style="grid-column:1/-1">
    <div class="card-title" style="display:flex;justify-content:space-between">
      <span>Journal des événements</span>
      <span id="log-count" style="color:var(--muted)">0 événements</span>
    </div>
    <div id="log-container"></div>
  </div>

</main>

<script>
const PHASES_LIST = [
  "Partition initiale", "Échantillonnage", "Repartitionnement",
  "Tri parallèle", "Fusion k-way", "Terminé"
];

let state = {
  status: 'idle', phase_idx: 0, total_elements: 0,
  num_workers: 0, workers: {}, phases_timing: {},
  partition_sizes: [], start_time: null, total_time: null
};
let startTs = null;
let timerInterval = null;
let logCount = 0;
let maxPartition = 0;

// ── Init phases track ──────────────────────────────────────────────────────
function buildPhasesTrack() {
  const track = document.getElementById('phases-track');
  track.innerHTML = PHASES_LIST.map((name, i) => `
    <div class="phase-step" id="phase-${i}">
      <div class="phase-dot">${i + 1}</div>
      <div class="phase-name">${name}</div>
      <div class="phase-time" id="ptime-${i}"></div>
    </div>
  `).join('');
}

function updatePhases(activeIdx, timings) {
  PHASES_LIST.forEach((name, i) => {
    const el = document.getElementById(`phase-${i}`);
    if (!el) return;
    el.className = 'phase-step';
    if (i < activeIdx) el.classList.add('done');
    else if (i === activeIdx) el.classList.add('active');

    const timeEl = document.getElementById(`ptime-${i}`);
    if (timings && timings[name]) {
      timeEl.textContent = timings[name].toFixed(3) + 's';
    }
  });
}

// ── Workers grid ───────────────────────────────────────────────────────────
function buildWorkersGrid(numWorkers) {
  const grid = document.getElementById('workers-grid');
  grid.innerHTML = '';
  for (let i = 0; i < numWorkers; i++) {
    grid.innerHTML += `
      <div class="worker-row" id="wr-${i}">
        <div class="worker-id">Worker ${i}</div>
        <div class="worker-bar-wrap"><div class="worker-bar" id="wbar-${i}"></div></div>
        <div class="worker-stat" id="welems-${i}" style="color:var(--muted)">—</div>
        <div class="worker-stat time" id="wtime-${i}">—</div>
        <div class="worker-status ws-waiting" id="wstatus-${i}">WAITING</div>
      </div>`;
  }
}

function updateWorker(id, data, maxElems) {
  const row = document.getElementById(`wr-${id}`);
  if (!row) return;
  row.className = 'worker-row done';
  const pct = maxElems > 0 ? (data.elements / maxElems * 100) : 0;
  document.getElementById(`wbar-${id}`).style.width = pct + '%';
  document.getElementById(`welems-${id}`).textContent =
    data.elements.toLocaleString('fr-FR');
  document.getElementById(`wtime-${id}`).textContent =
    data.sort_time.toFixed(4) + 's';
  const ws = document.getElementById(`wstatus-${id}`);
  ws.textContent = 'DONE'; ws.className = 'worker-status ws-done';
}

function setWorkerSorting(id) {
  const row = document.getElementById(`wr-${id}`);
  if (!row) return;
  row.className = 'worker-row active';
  const ws = document.getElementById(`wstatus-${id}`);
  ws.textContent = 'SORTING'; ws.className = 'worker-status ws-sorting';
}

// ── Partition bar chart ────────────────────────────────────────────────────
function renderPartitionChart(sizes) {
  const chart = document.getElementById('bar-chart');
  if (!sizes || sizes.length === 0) return;
  const max = Math.max(...sizes);
  chart.innerHTML = sizes.map((v, i) => `
    <div class="bar-col">
      <div class="bar-val">${(v/1000).toFixed(0)}k</div>
      <div class="bar-fill" style="height:${Math.round(v/max*120)}px"></div>
      <div class="bar-label">W${i}</div>
    </div>
  `).join('');
}

// ── Stats update ───────────────────────────────────────────────────────────
function updateStats() {
  if (state.total_elements)
    document.getElementById('s-elements').textContent =
      state.total_elements.toLocaleString('fr-FR');
  if (state.num_workers)
    document.getElementById('s-workers').textContent = state.num_workers;

  const doneCount = Object.values(state.workers).filter(w => w.status === 'done').length;
  document.getElementById('s-workers-done').textContent =
    `${doneCount} / ${state.num_workers || '?'} terminés`;

  if (state.phase_idx !== undefined)
    document.getElementById('s-phase').textContent =
      PHASES_LIST[state.phase_idx] || '—';

  if (state.total_time && state.total_elements) {
    const tp = Math.round(state.total_elements / state.total_time);
    document.getElementById('s-throughput').textContent =
      tp.toLocaleString('fr-FR');
  }

  updatePhases(state.phase_idx, state.phases_timing);
}

// ── Timer ──────────────────────────────────────────────────────────────────
function startTimer() {
  startTs = Date.now();
  if (timerInterval) clearInterval(timerInterval);
  timerInterval = setInterval(() => {
    if (state.status !== 'running') return;
    const elapsed = (Date.now() - startTs) / 1000;
    document.getElementById('total-time-display').textContent =
      elapsed.toFixed(3) + 's';
    document.getElementById('live-timer').textContent = '⏱ En cours...';
  }, 50);
}

function stopTimer(finalTime) {
  if (timerInterval) clearInterval(timerInterval);
  document.getElementById('total-time-display').textContent =
    finalTime.toFixed(3) + 's';
  document.getElementById('live-timer').textContent = '✓ Terminé';
}

// ── Badge ──────────────────────────────────────────────────────────────────
function setBadge(status) {
  const el = document.getElementById('status-badge');
  el.className = 'badge-' + status;
  const labels = {idle:'● IDLE', running:'● RUNNING', done:'● DONE', error:'● ERROR'};
  el.textContent = labels[status] || status.toUpperCase();
}

// ── Log ────────────────────────────────────────────────────────────────────
function addLog(evType, msg) {
  logCount++;
  document.getElementById('log-count').textContent = logCount + ' événements';
  const now = new Date().toISOString().slice(11, 23);
  const colors = {
    start:'ev-start', phase:'ev-phase', worker_done:'ev-worker',
    done:'ev-done', error:'ev-error', pivots:'ev-pivot', state:'ev-start'
  };
  const cls = colors[evType] || '';
  const icons = {
    start:'🚀', phase:'⚡', worker_done:'✓', done:'🎉', error:'✗', pivots:'📐', state:'ℹ'
  };
  const icon = icons[evType] || '·';
  const container = document.getElementById('log-container');
  const line = document.createElement('div');
  line.className = 'log-line';
  line.innerHTML = `<span class="log-ts">${now}</span><span class="log-ev ${cls}">${icon} [${evType}]</span>${msg}`;
  container.appendChild(line);
  container.scrollTop = container.scrollHeight;
}

// ── SSE connection ─────────────────────────────────────────────────────────
function applyState(s) {
  state = { ...state, ...s };
  setBadge(state.status);
  updateStats();
  if (state.num_workers > 0) buildWorkersGrid(state.num_workers);
  if (state.partition_sizes && state.partition_sizes.length)
    renderPartitionChart(state.partition_sizes);
  const maxElems = Math.max(...Object.values(state.workers).map(w=>w.elements||0), 1);
  Object.entries(state.workers).forEach(([id, data]) => {
    if (data.status === 'done') updateWorker(parseInt(id), data, maxElems);
  });
  if (state.total_time) stopTimer(state.total_time);
  else if (state.status === 'running') startTimer();
}

function connect() {
  const es = new EventSource('/api/events');
  let _readyPinged = false;
  es.onmessage = (e) => {
    // Signal Python que le browser est prêt (une seule fois)
    if (!_readyPinged) {
      _readyPinged = true;
      fetch('/api/ready').catch(() => {});
    }

    const payload = JSON.parse(e.data);
    const { event: ev, data } = payload;

    switch (ev) {
      case 'state':
        applyState(data);
        addLog('state', `État initial chargé — status: ${data.status}`);
        break;

      case 'start':
        state.status = 'running';
        state.total_elements = data.total_elements;
        state.num_workers = data.num_workers;
        state.workers = {};
        for (let i = 0; i < data.num_workers; i++)
          state.workers[i] = {elements:0, sort_time:0, memory_kb:0, status:'waiting'};
        setBadge('running');
        buildWorkersGrid(data.num_workers);
        startTimer();
        document.getElementById('s-elements').textContent =
          data.total_elements.toLocaleString('fr-FR');
        document.getElementById('s-workers').textContent = data.num_workers;
        document.getElementById('s-workers-info').textContent =
          `${data.num_workers} workers connectés`;
        addLog('start', `Tri démarré — ${data.total_elements.toLocaleString('fr-FR')} éléments sur ${data.num_workers} workers`);
        break;

      case 'phase':
        state.phase_idx = data.phase_idx;
        state.phase = data.phase;
        if (data.duration) state.phases_timing[data.phase] = data.duration;
        updatePhases(data.phase_idx, state.phases_timing);
        document.getElementById('s-phase').textContent = data.phase;
        // Mark workers as sorting when entering sort phase
        if (data.phase === 'Tri parallèle') {
          for (let i = 0; i < state.num_workers; i++) setWorkerSorting(i);
        }
        const dur = data.duration ? ` (${data.duration.toFixed(3)}s)` : '';
        addLog('phase', `Phase → ${data.phase}${dur}`);
        break;

      case 'pivots':
        state.partition_sizes = data.partition_sizes;
        state.pivots = data.pivots;
        renderPartitionChart(data.partition_sizes);
        maxPartition = Math.max(...data.partition_sizes);
        addLog('pivots', `Pivots: [${data.pivots.map(p=>p.toLocaleString('fr-FR')).join(', ')}]`);
        break;

      case 'worker_done':
        state.workers[data.worker_id] = {
          elements: data.elements, sort_time: data.sort_time,
          memory_kb: data.memory_kb, status: 'done'
        };
        updateWorker(data.worker_id, state.workers[data.worker_id], maxPartition);
        const doneN = Object.values(state.workers).filter(w=>w.status==='done').length;
        document.getElementById('s-workers-done').textContent =
          `${doneN} / ${state.num_workers} terminés`;
        addLog('worker_done',
          `Worker ${data.worker_id} — ${data.elements.toLocaleString('fr-FR')} élém. en ${data.sort_time.toFixed(4)}s — ${data.memory_kb.toLocaleString('fr-FR')} KB`);
        break;

      case 'done':
        state.status = 'done';
        state.total_time = data.total_time;
        setBadge('done');
        stopTimer(data.total_time);
        if (state.total_elements) {
          const tp = Math.round(state.total_elements / data.total_time);
          document.getElementById('s-throughput').textContent = tp.toLocaleString('fr-FR');
        }
        updatePhases(5, state.phases_timing);
        addLog('done', `✓ Tri terminé en ${data.total_time.toFixed(4)}s`);
        break;

      case 'error':
        state.status = 'error';
        setBadge('error');
        addLog('error', data.msg);
        break;
    }
  };

  es.onerror = () => {
    addLog('error', 'Connexion SSE perdue — reconnexion dans 3s...');
    es.close();
    setTimeout(connect, 3000);
  };
}

// ── Init ───────────────────────────────────────────────────────────────────
buildPhasesTrack();
updatePhases(0, {});
connect();
</script>
</body>
</html>
"""
