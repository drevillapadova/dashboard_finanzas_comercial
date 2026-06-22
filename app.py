import os, json, base64, tempfile, threading
from datetime import datetime
from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app  = Flask(__name__)
LIMA = pytz.timezone("America/Lima")

SPREADSHEET_ID  = os.environ.get("GSHEETS_SPREADSHEET_ID", "")
CREDENTIALS_B64 = os.environ.get("GSHEETS_CREDENTIALS_B64", "")

TABS = ["VENTAS", "STOCK", "INGRESO_DEPOSITO", "FLUJO_CAJA"]

_cache = {t: [] for t in TABS}
_cache["updated_at"] = None


# ── Google Sheets client ──────────────────────────────────────

def _gsheets_client():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    if CREDENTIALS_B64:
        creds_dict = json.loads(base64.b64decode(CREDENTIALS_B64).decode("utf-8"))
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(creds_dict, tmp); tmp.flush()
        creds_file = tmp.name
    else:
        creds_file = os.path.join(os.path.dirname(__file__), "evoltareportes-00ffe1b337be.json")
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    return gspread.authorize(creds)


# ── Cache ─────────────────────────────────────────────────────

def actualizar_cache():
    ts = datetime.now(LIMA).strftime("%H:%M:%S")
    print(f"\n[{ts}] Actualizando cache desde Google Sheets...")
    try:
        client = _gsheets_client()
        sh     = client.open_by_key(SPREADSHEET_ID)
        for tab in TABS:
            try:
                rows = sh.worksheet(tab).get_all_values()
                _cache[tab] = rows
                print(f"   -> {tab}: {max(0, len(rows)-1):,} filas")
            except Exception as e:
                print(f"   !! Error leyendo {tab}: {e}")
    except Exception as e:
        print(f"!! Error conectando a Sheets: {e}")
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
