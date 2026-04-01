"""
Tamil2Lyrics — Unified Control Panel Server
============================================
Flask server that serves the dashboard UI and manages scraper/importer
as background subprocesses. No terminal needed.

Usage:
    python dashboard.py
    → open http://localhost:8080
"""

import json
import subprocess
import sys
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
CONFIG_FILE        = OUTPUT_DIR / "config.json"
SCRAPER_PROGRESS   = OUTPUT_DIR / "scraper_progress.json"
IMPORTER_PROGRESS  = OUTPUT_DIR / "progress.json"

OUTPUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------

_processes: dict[str, subprocess.Popen | None] = {"scraper": None, "importer": None}
_proc_lock = threading.Lock()

# Windows flag: suppress console popup when spawning subprocesses
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _is_running(name: str) -> bool:
    p = _processes[name]
    return p is not None and p.poll() is None


def _start(name: str) -> tuple[int | None, str | None]:
    with _proc_lock:
        if _is_running(name):
            return None, "already running"

        log_path = OUTPUT_DIR / f"{name}.log"
        log_file = open(log_path, "a", encoding="utf-8", buffering=1)

        p = subprocess.Popen(
            [sys.executable, f"{name}.py"],
            cwd=str(ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            creationflags=_NO_WINDOW,
        )
        _processes[name] = p
        return p.pid, None


def _stop(name: str) -> str:
    with _proc_lock:
        p = _processes[name]
        if not p or p.poll() is not None:
            return "not running"
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        _processes[name] = None
        return "stopped"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json(path: Path) -> dict | None:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def _tail_log(name: str, lines: int = 100) -> list[str]:
    log_path = OUTPUT_DIR / f"{name}.log"
    try:
        if not log_path.exists():
            return []
        all_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return all_lines[-lines:]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__, static_folder=str(ROOT))


@app.route("/")
def index():
    return send_from_directory(str(ROOT), "dashboard.html")


# ── Status ────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    result = {}
    for name in ("scraper", "importer"):
        p = _processes[name]
        running = _is_running(name)
        pid     = p.pid if running and p else None

        prog_file = SCRAPER_PROGRESS if name == "scraper" else IMPORTER_PROGRESS
        progress  = _read_json(prog_file)

        result[name] = {"running": running, "pid": pid, "progress": progress}

    return jsonify(result)


# ── Process control ───────────────────────────────────────────────────────

@app.route("/api/scraper/start", methods=["POST"])
def api_scraper_start():
    pid, err = _start("scraper")
    if err:
        return jsonify({"error": err}), 409
    return jsonify({"ok": True, "pid": pid})


@app.route("/api/scraper/stop", methods=["POST"])
def api_scraper_stop():
    msg = _stop("scraper")
    return jsonify({"ok": True, "message": msg})


@app.route("/api/importer/start", methods=["POST"])
def api_importer_start():
    pid, err = _start("importer")
    if err:
        return jsonify({"error": err}), 409
    return jsonify({"ok": True, "pid": pid})


@app.route("/api/importer/stop", methods=["POST"])
def api_importer_stop():
    msg = _stop("importer")
    return jsonify({"ok": True, "message": msg})


# ── Config ────────────────────────────────────────────────────────────────

@app.route("/api/config")
def api_config_get():
    cfg = _read_json(CONFIG_FILE) or {}
    if "password" in cfg:
        cfg["password"] = "***"
    return jsonify(cfg)


@app.route("/api/config/save", methods=["POST"])
def api_config_save():
    data = request.get_json(silent=True) or {}
    required = {"host", "port", "user", "password", "database", "prefix"}
    missing = required - data.keys()
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    # Don't overwrite password if placeholder sent
    existing = _read_json(CONFIG_FILE) or {}
    if data.get("password") == "***" and existing.get("password"):
        data["password"] = existing["password"]

    CONFIG_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return jsonify({"ok": True})


# ── Logs ──────────────────────────────────────────────────────────────────

@app.route("/api/logs/<name>")
def api_logs(name: str):
    if name not in ("scraper", "importer"):
        return jsonify({"error": "unknown"}), 400
    lines = int(request.args.get("lines", 100))
    return jsonify({"lines": _tail_log(name, lines)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 9000))
    print("=" * 50)
    print("Tamil2Lyrics Dashboard")
    print(f"Open: http://localhost:{port}")
    print("=" * 50)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
