import io, threading, requests
from datetime import datetime
from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import pytz

app  = Flask(__name__)
LIMA = pytz.timezone("America/Lima")

SHEET_ID = "18uWdlUjIf1v9n4RSuEw3AK-LBseTtD78jNZtLTanF2U"
TABS     = ["VENTAS", "STOCK", "INGRESO_DEPOSITO", "FLUJO_CAJA"]

_cache = {t: [] for t in TABS}
_cache["updated_at"] = None


# ── Lectura de Sheets ─────────────────────────────────────────

def csv_url(tab_name):
    from time import time
    return (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            f"/gviz/tq?tqx=out:csv&sheet={tab_name}&ts={int(time())}")


def leer_tab(tab_name):
    try:
        resp = requests.get(csv_url(tab_name), timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False)
        rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
        print(f"   -> {tab_name}: {len(rows)-1:,} filas")
        return rows
    except Exception as e:
        print(f"   !! Error leyendo {tab_name}: {e}")
        return []


# ── Cache ─────────────────────────────────────────────────────

def actualizar_cache():
    ts = datetime.now(LIMA).strftime("%H:%M:%S")
    print(f"\n[{ts}] Actualizando cache desde Google Sheets...")
    for tab in TABS:
        _cache[tab] = leer_tab(tab)
    _cache["updated_at"] = datetime.now(LIMA).strftime("%d/%m/%Y %H:%M")
    print(f"   -> Cache OK · {_cache['updated_at']}")


# ── Endpoints ─────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    return jsonify({
        "ventas":               _cache["VENTAS"],
        "stock":                _cache["STOCK"],
        "ing_deposito":         _cache["INGRESO_DEPOSITO"],
        "flujo_caja":           _cache["FLUJO_CAJA"],
        "ultima_actualizacion": _cache["updated_at"],
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    actualizar_cache()
    return jsonify({"ok": True, "updated_at": _cache["updated_at"]})


# ── Arranque ──────────────────────────────────────────────────

threading.Thread(target=actualizar_cache, daemon=True).start()

scheduler = BackgroundScheduler(timezone=LIMA)
scheduler.add_job(actualizar_cache, "interval", hours=1)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
