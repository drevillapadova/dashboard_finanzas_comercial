import os, threading, tempfile, shutil
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import pytz
from sharepoint_client import SharePointClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app  = Flask(__name__)
LIMA = pytz.timezone("America/Lima")

SHAREPOINT_DASHBOARD_FOLDER = os.environ.get(
    "SHAREPOINT_DASHBOARD_FOLDER",
    "FINANZAS/TI/AUTOMATIZACIONES/DASHBOARD_FINANZAS_COMERCIAL",
)
SHAREPOINT_DASHBOARD_FILENAME = os.environ.get(
    "SHAREPOINT_DASHBOARD_FILENAME", "Dashboard_Finanzas_Comercial.xlsx"
)
TABS = ["VENTAS", "STOCK", "FLUJO_CAJA"]

_cache = {t: [] for t in TABS}
_cache["updated_at"] = None


# ── Lectura desde SharePoint ──────────────────────────────────

def leer_excel_sharepoint():
    """Descarga el Excel del dashboard desde SharePoint y devuelve un dict
    {tab_name: rows} en el mismo formato que antes (filas + encabezado)."""
    tmp_dir = tempfile.mkdtemp(prefix="dashboard_fc_read_")
    try:
        client = SharePointClient()
        remote_path = f"{SHAREPOINT_DASHBOARD_FOLDER}/{SHAREPOINT_DASHBOARD_FILENAME}"
        local_path = client.download_to(remote_path, Path(tmp_dir) / SHAREPOINT_DASHBOARD_FILENAME)

        resultado = {}
        for tab_name in TABS:
            try:
                df = pd.read_excel(local_path, sheet_name=tab_name)
                rows = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
                print(f"   -> {tab_name}: {len(rows)-1:,} filas")
                resultado[tab_name] = rows
            except Exception as e:
                print(f"   !! Error leyendo hoja {tab_name}: {e}")
                resultado[tab_name] = []
        return resultado
    except Exception as e:
        print(f"   !! Error descargando de SharePoint: {e}")
        return {t: [] for t in TABS}
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ── Cache ─────────────────────────────────────────────────────

def actualizar_cache():
    ts = datetime.now(LIMA).strftime("%H:%M:%S")
    print(f"\n[{ts}] Actualizando cache desde SharePoint...")
    datos = leer_excel_sharepoint()
    for tab in TABS:
        _cache[tab] = datos[tab]
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
        "ing_deposito":         _cache["FLUJO_CAJA"],
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
