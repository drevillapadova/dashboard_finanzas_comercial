"""
ETL Finanzas-Comercial — Padova SAC
Descarga 4 reportes de Evolta y los sube a Google Sheets:
  - VENTAS
  - STOCK (separaciones)
  - INGRESO_DEPOSITO
  - FLUJO_CAJA  ← nuevo

Basado en ETL_Padova_MultiRol.py. Reutiliza misma lógica de
login, descarga, tipo de cambio y upload a Sheets.
"""

import time, os, glob, json, shutil, requests, traceback
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials as ServiceCredentials
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

# ============================================================
# TIPO DE CAMBIO
# ============================================================

_TC_CACHE = {}

def _fetch_tc_eapi(fecha_str):
    try:
        r = requests.get(f"https://free.e-api.net.pe/tipo-cambio/{fecha_str}.json", timeout=10)
        data = r.json()
        return float(data["venta"]) if data.get("venta") else None
    except: return None

def _fetch_tc_bcrp(fecha_str):
    try:
        url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha_str}/{fecha_str}/ing"
        r = requests.get(url, timeout=10)
        data = json.loads(r.content.decode('utf-8-sig'))
        periodos = data.get("periods", [])
        if periodos and periodos[0].get("values"):
            return float(periodos[0]["values"][0])
    except: return None

def get_tipo_cambio(fecha=None):
    TC_RESPALDO = 3.75
    import pandas as _pd
    if fecha is None or (hasattr(_pd, 'isnull') and _pd.isnull(fecha)):
        fecha_dt = datetime.now()
    elif isinstance(fecha, str):
        try: fecha_dt = datetime.strptime(fecha[:10], "%Y-%m-%d")
        except: fecha_dt = datetime.now()
    elif hasattr(fecha, 'strftime'):
        try: fecha_dt = fecha.to_pydatetime() if hasattr(fecha, 'to_pydatetime') else fecha
        except: fecha_dt = datetime.now()
    else:
        fecha_dt = datetime.now()

    fecha_str = fecha_dt.strftime("%Y-%m-%d")
    if fecha_str in _TC_CACHE: return _TC_CACHE[fecha_str]

    for dias_atras in range(0, 8):
        f = (fecha_dt - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
        if f in _TC_CACHE:
            _TC_CACHE[fecha_str] = _TC_CACHE[f]
            return _TC_CACHE[f]
        tc_raw = _fetch_tc_eapi(f) or _fetch_tc_bcrp(f)
        if tc_raw:
            tc = round(tc_raw, 2)
            print(f"   -> [TC] {fecha_str}: S/ {tc}")
            _TC_CACHE[fecha_str] = tc
            _TC_CACHE[f] = tc
            return tc

    _TC_CACHE[fecha_str] = TC_RESPALDO
    return TC_RESPALDO


def convertir_monedas(df, col_precio, col_moneda, col_fecha=None):
    """
    Agrega PrecioOriginal, PrecioSoles y PrecioDolares.
    PrecioOriginal = el valor tal cual está en Evolta (en su moneda original).
    PrecioSoles    = convertido a soles con TC SUNAT de la fecha del registro.
    PrecioDolares  = convertido a dólares con TC SUNAT de la fecha del registro.
    """
    df = df.copy()
    orig, soles_list, dolar_list = [], [], []

    def _tc(fecha_val):
        return get_tipo_cambio(fecha_val) if fecha_val else get_tipo_cambio()

    for _, row in df.iterrows():
        try: precio = float(str(row[col_precio]).replace(",", "")) if row[col_precio] else 0
        except: precio = 0

        moneda = str(row.get(col_moneda, "")).upper().strip()
        es_usd = "DOLAR" in moneda or "USD" in moneda
        fecha_val = row.get(col_fecha) if col_fecha and col_fecha in df.columns else None
        tc = _tc(fecha_val)

        orig.append(precio)
        if es_usd:
            soles_list.append(round(precio * tc, 2))
            dolar_list.append(round(precio, 2))
        else:
            soles_list.append(round(precio, 2))
            dolar_list.append(round(precio / tc, 2) if tc else 0)

    df["PrecioOriginal"] = orig
    df["PrecioSoles"]    = soles_list
    df["PrecioDolares"]  = dolar_list
    return df


# ============================================================
# CONFIGURACIÓN
# ============================================================

USER_CRED = os.environ.get("EVOLTA_USER", "calopez")
PASS_CRED = os.environ.get("EVOLTA_PASS", "")
EMAIL_FROM = "sistema.padova@gmail.com"
EMAIL_PASS = os.environ.get("EMAIL_PASS", "")

URL_LOGIN                = "https://v4.evolta.pe/Login/Acceso/Index"
URL_REPORTE_STOCK        = "https://v4.evolta.pe/Reportes/RepCargaStock/IndexNuevoRepStock"
URL_REPORTE_VENTAS       = "https://v4.evolta.pe/Reportes/RepVenta/Index"
URL_REPORTE_ING_DEPOSITO = "https://v4.evolta.pe/Reportes/RepIngresoxDeposito/Index"
URL_REPORTE_FLUJO_CAJA   = "https://v4.evolta.pe/Reportes/RepFlujoCaga/Index"

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900', 'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO', 'DOMINGO ORUE'
]

IS_CLOUD = os.name != 'nt'

if IS_CLOUD:
    DOWNLOAD_DIR          = "/tmp/fc_stock"
    DOWNLOAD_DIR_VENTAS   = "/tmp/fc_ventas"
    DOWNLOAD_DIR_ING_DEP  = "/tmp/fc_ing_dep"
    DOWNLOAD_DIR_FLUJO    = "/tmp/fc_flujo"
else:
    DOWNLOAD_DIR          = r"C:\Users\MKT\Documents\EVOLTA\fc_stock"
    DOWNLOAD_DIR_VENTAS   = r"C:\Users\MKT\Documents\EVOLTA\fc_ventas"
    DOWNLOAD_DIR_ING_DEP  = r"C:\Users\MKT\Documents\EVOLTA\fc_ing_dep"
    DOWNLOAD_DIR_FLUJO    = r"C:\Users\MKT\Documents\EVOLTA\fc_flujo"

# ⬇ Cambia este ID por el del nuevo Google Sheet que crees para este dashboard
GSHEETS_SPREADSHEET_ID = "TU_NUEVO_SHEET_ID_AQUI"

AÑOS = [2023, 2024, 2025, 2026]

for d in [DOWNLOAD_DIR, DOWNLOAD_DIR_VENTAS, DOWNLOAD_DIR_ING_DEP, DOWNLOAD_DIR_FLUJO]:
    os.makedirs(d, exist_ok=True)


def _load_gsheets_credentials():
    import base64, tempfile
    b64 = os.environ.get("GSHEETS_CREDENTIALS_B64", "")
    if b64:
        creds_dict = json.loads(base64.b64decode(b64).decode("utf-8"))
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(creds_dict, tmp); tmp.flush()
        return tmp.name
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evoltareportes-00ffe1b337be.json")
    if os.path.exists(local_path): return local_path
    raise FileNotFoundError("No se encontraron credenciales de Google.")

GSHEETS_CREDENTIALS_FILE = _load_gsheets_credentials()


# ============================================================
# SELENIUM — helpers
# ============================================================

def get_driver(download_dir):
    os.makedirs(download_dir, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--log-level=3")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=options) if IS_CLOUD else webdriver.Chrome(options=options)


def dismiss_popup(driver):
    try:
        driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        time.sleep(1)
    except: pass


def robust_login(driver, wait):
    print(">> [LOGIN] Iniciando...")
    driver.get(URL_LOGIN)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    try: user_field = driver.find_element(By.ID, "UserName")
    except:
        try: user_field = driver.find_element(By.NAME, "Usuario")
        except: user_field = driver.find_element(By.XPATH, "//input[@type='text']")
    user_field.clear()
    user_field.send_keys(USER_CRED)
    driver.find_element(By.XPATH, "//input[@type='password']").send_keys(PASS_CRED)
    try: driver.find_element(By.XPATH, "//button[@type='submit'] | //input[@type='submit']").click()
    except: pass
    time.sleep(3)
    dismiss_popup(driver)
    print(">> [LOGIN] OK")


def esperar_descarga_nueva(watch_dir, existing, timeout=300):
    elapsed = 0
    while elapsed < timeout:
        current = set(glob.glob(os.path.join(watch_dir, "*.*")))
        nuevos = [f for f in (current - existing)
                  if not f.endswith(('.crdownload', '.tmp')) and os.path.getsize(f) > 0]
        if nuevos:
            return nuevos[0]
        time.sleep(1); elapsed += 1
    return None


def _set_fechas_js(driver, fecha_inicio, fecha_fin):
    driver.execute_script(f"""
        var inputs = document.querySelectorAll('input');
        var df = [];
        for(var i=0;i<inputs.length;i++){{
            var v=inputs[i].value||'';
            if(v.match(/\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/)) df.push(inputs[i]);
        }}
        if(df.length>=2){{
            df[0].value='{fecha_inicio}'; df[0].dispatchEvent(new Event('change',{{bubbles:true}}));
            df[1].value='{fecha_fin}';    df[1].dispatchEvent(new Event('change',{{bubbles:true}}));
        }}
    """)
    time.sleep(1)


def _click_exportar(driver, wait):
    for xpath in ["//button[contains(text(),'Exportar')]", "//button[@id='btnExportar']", "//button[@type='submit']"]:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].click();", btn)
            print("   -> Click en Exportar")
            return
        except: pass


def _mover_descarga(archivo, destino):
    if os.path.exists(destino): os.remove(destino)
    shutil.move(archivo, destino)
    print(f"   -> [OK] {os.path.basename(destino)}")


# ============================================================
# EXTRACCIÓN — STOCK
# ============================================================

def execute_stock_extraction(driver, wait):
    print("\n>> [STOCK] Descargando...")
    driver.get(URL_REPORTE_STOCK)
    time.sleep(3); dismiss_popup(driver)
    try:
        sel = wait.until(EC.presence_of_element_located((By.ID, "ProyectoId")))
        try: Select(sel).select_by_visible_text("Todos")
        except: Select(sel).select_by_index(0)
    except: pass
    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing)
    if archivo:
        dest = os.path.join(DOWNLOAD_DIR, "ReporteStock.xlsx")
        _mover_descarga(archivo, dest)
    else:
        print("   !! No se descargó stock")


# ============================================================
# EXTRACCIÓN — VENTAS (por año)
# ============================================================

def execute_ventas_año(driver, wait, año):
    print(f"\n>> [VENTAS {año}]")
    driver.get(URL_REPORTE_VENTAS)
    time.sleep(4); dismiss_popup(driver)
    fecha_inicio = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")
    _set_fechas_js(driver, fecha_inicio, fecha_fin)
    # Seleccionar CSV si está disponible
    try:
        csv_radio = driver.find_element(By.XPATH, "//input[@type='radio'][@value='Csv' or @value='csv' or @value='CSV']")
        driver.execute_script("arguments[0].click();", csv_radio)
    except: pass
    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=120)
    if archivo:
        ext = os.path.splitext(archivo)[1].lower()
        dest = os.path.join(DOWNLOAD_DIR_VENTAS, f"ReporteVenta{año}{ext}")
        _mover_descarga(archivo, dest)
    else:
        print(f"   !! No se descargó ventas {año}")

def execute_ventas_extraction(driver, wait):
    print("\n" + "="*60)
    print(">> [VENTAS] Iniciando descarga por año")
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.*")):
        try: os.remove(f)
        except: pass
    for año in AÑOS:
        try: execute_ventas_año(driver, wait, año); time.sleep(2)
        except Exception as e: print(f"   !! Error ventas {año}: {e}")


# ============================================================
# EXTRACCIÓN — INGRESO POR DEPÓSITO (por año)
# ============================================================

def execute_ing_deposito_año(driver, wait, año):
    print(f"\n>> [INGRESO_DEPOSITO {año}]")
    driver.get(URL_REPORTE_ING_DEPOSITO)
    time.sleep(4); dismiss_popup(driver)
    # Seleccionar VENTA en Etapa Comercial si existe
    try:
        for sel in driver.find_elements(By.TAG_NAME, "select"):
            opts = [o.text.strip().upper() for o in sel.find_elements(By.TAG_NAME, "option")]
            if "VENTA" in opts:
                Select(sel).select_by_visible_text("VENTA"); time.sleep(0.5); break
    except: pass
    fecha_inicio = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")
    _set_fechas_js(driver, fecha_inicio, fecha_fin)
    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=480)
    if archivo:
        ext = os.path.splitext(archivo)[1].lower()
        dest = os.path.join(DOWNLOAD_DIR_ING_DEP, f"ReporteIngresoDeposito{año}{ext}")
        _mover_descarga(archivo, dest)
    else:
        print(f"   !! No se descargó ingreso_deposito {año}")

def execute_ing_deposito_extraction(driver, wait):
    print("\n" + "="*60)
    print(">> [INGRESO_DEPOSITO] Iniciando descarga por año")
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_ING_DEP, "*.*")):
        try: os.remove(f)
        except: pass
    for año in AÑOS:
        try: execute_ing_deposito_año(driver, wait, año); time.sleep(2)
        except Exception as e: print(f"   !! Error ing_deposito {año}: {e}")


# ============================================================
# EXTRACCIÓN — FLUJO DE CAJA (por año)  ← NUEVO
# ============================================================

def execute_flujo_caja_año(driver, wait, año):
    """
    Descarga el reporte de Flujo de Caja para un año.
    URL: https://v4.evolta.pe/Reportes/RepFlujoCaga/Index
    La lógica replica ingreso_deposito: filtro de fechas + exportar.
    NOTA: Revisar en Evolta si el reporte tiene filtros adicionales
    (por proyecto, tipo de movimiento, etc.) y agregar aquí si es necesario.
    """
    print(f"\n>> [FLUJO_CAJA {año}]")
    driver.get(URL_REPORTE_FLUJO_CAJA)
    time.sleep(4); dismiss_popup(driver)

    # Seleccionar proyecto TODOS si hay dropdown
    try:
        selects = driver.find_elements(By.TAG_NAME, "select")
        for sel in selects:
            opts = [o.text.strip().upper() for o in sel.find_elements(By.TAG_NAME, "option")]
            if "TODOS" in opts or "TODO" in opts:
                try: Select(sel).select_by_visible_text("Todos")
                except: Select(sel).select_by_index(0)
                time.sleep(0.5)
                break
    except: pass

    fecha_inicio = f"01/01/{año}"
    fecha_fin = f"31/12/{año}" if año < datetime.now().year else datetime.now().strftime("%d/%m/%Y")
    _set_fechas_js(driver, fecha_inicio, fecha_fin)

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR, existing, timeout=480)
    if archivo:
        ext = os.path.splitext(archivo)[1].lower()
        dest = os.path.join(DOWNLOAD_DIR_FLUJO, f"ReporteFlujoCaja{año}{ext}")
        _mover_descarga(archivo, dest)
    else:
        print(f"   !! No se descargó flujo_caja {año}")
        # Guardar screenshot para debug
        driver.save_screenshot(os.path.join(DOWNLOAD_DIR_FLUJO, f"debug_flujo_{año}.png"))

def execute_flujo_caja_extraction(driver, wait):
    print("\n" + "="*60)
    print(">> [FLUJO_CAJA] Iniciando descarga por año")
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_FLUJO, "*.*")):
        try: os.remove(f)
        except: pass
    for año in AÑOS:
        try: execute_flujo_caja_año(driver, wait, año); time.sleep(2)
        except Exception as e: print(f"   !! Error flujo_caja {año}: {e}")


# ============================================================
# TRANSFORMACIÓN — helpers
# ============================================================

def _leer_por_año(directorio, prefijo, años):
    dfs = []
    for año in años:
        for ext in ['.csv', '.xlsx']:
            ruta = os.path.join(directorio, f"{prefijo}{año}{ext}")
            if not os.path.exists(ruta): continue
            try:
                df = pd.read_csv(ruta, encoding='utf-8', low_memory=False) if ext == '.csv' else pd.read_excel(ruta)
                df['AÑO'] = año
                dfs.append(df)
                print(f"   -> {prefijo}{año}: {len(df):,} filas")
                break
            except Exception as e:
                print(f"   !! Error leyendo {ruta}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else None


def _filtrar_proyectos(df, col='Proyecto'):
    if col in df.columns:
        return df[df[col].str.upper().isin(TARGET_PROJECTS)]
    return df


# ============================================================
# TRANSFORMACIÓN — VENTAS
# (reutiliza lógica de dashboard_separacionesyventas)
# ============================================================

def process_ventas(df_stock_crudo=None):
    print("\n>> [TRANSFORM VENTAS]")
    df = _leer_por_año(DOWNLOAD_DIR_VENTAS, "ReporteVenta", AÑOS)
    if df is None: return None
    df = _filtrar_proyectos(df)

    # Tipo de cambio por fecha de venta
    col_fecha = next((c for c in ['FechaVenta', 'FechaEntrega_Minuta'] if c in df.columns), None)
    col_moneda = 'TipoMoneda' if 'TipoMoneda' in df.columns else None
    col_precio = 'PrecioVenta' if 'PrecioVenta' in df.columns else None

    if col_precio and col_moneda:
        df = convertir_monedas(df, col_precio, col_moneda, col_fecha)

    print(f"   -> VENTAS procesadas: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — STOCK (separaciones)
# ============================================================

def process_stock():
    print("\n>> [TRANSFORM STOCK]")
    archivos = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
    if not archivos: return None
    df = pd.read_excel(max(archivos, key=os.path.getctime))
    df.columns = df.columns.str.strip()
    df = _filtrar_proyectos(df)

    col_precio = next((c for c in ['PrecioVenta', 'PrecioLista'] if c in df.columns), None)
    col_moneda = 'Moneda' if 'Moneda' in df.columns else None
    col_fecha  = next((c for c in ['FechaSepDefinitiva', 'FechaVenta'] if c in df.columns), None)

    if col_precio and col_moneda:
        df = convertir_monedas(df, col_precio, col_moneda, col_fecha)

    print(f"   -> STOCK procesado: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — INGRESO DEPÓSITO
# ============================================================

def process_ing_deposito():
    print("\n>> [TRANSFORM INGRESO_DEPOSITO]")
    df = _leer_por_año(DOWNLOAD_DIR_ING_DEP, "ReporteIngresoDeposito", AÑOS)
    if df is None: return None
    df = _filtrar_proyectos(df)

    # Detectar columnas de monto y moneda dinámicamente
    col_monto  = next((c for c in df.columns if 'monto' in c.lower() or 'importe' in c.lower()), None)
    col_moneda = next((c for c in df.columns if 'moneda' in c.lower() or 'tipo' in c.lower()), None)
    col_fecha  = next((c for c in df.columns if 'fecha' in c.lower()), None)

    if col_monto and col_moneda:
        df = convertir_monedas(df, col_monto, col_moneda, col_fecha)

    print(f"   -> INGRESO_DEPOSITO procesado: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — FLUJO DE CAJA  ← NUEVO
# ============================================================

def process_flujo_caja():
    """
    Lee y procesa el reporte de Flujo de Caja.
    NOTA: Las columnas exactas se conocerán cuando Evolta exporte
    el reporte por primera vez. Esta función las detecta dinámicamente.
    Ajustar col_monto / col_moneda / col_fecha una vez que se vea el Excel.
    """
    print("\n>> [TRANSFORM FLUJO_CAJA]")
    df = _leer_por_año(DOWNLOAD_DIR_FLUJO, "ReporteFlujoCaja", AÑOS)
    if df is None:
        print("   !! Sin datos de flujo de caja todavía")
        return None

    df = _filtrar_proyectos(df)

    # Detectar columnas dinámicamente — ajustar cuando se vea el reporte real
    col_monto  = next((c for c in df.columns if any(k in c.lower() for k in ['monto', 'importe', 'cuota', 'pago'])), None)
    col_moneda = next((c for c in df.columns if any(k in c.lower() for k in ['moneda', 'tipo_mon'])), None)
    col_fecha  = next((c for c in df.columns if any(k in c.lower() for k in ['fecha', 'vencim'])), None)

    if col_monto and col_moneda:
        df = convertir_monedas(df, col_monto, col_moneda, col_fecha)
    elif col_monto:
        # Si no hay columna moneda, asumir soles
        df['PrecioOriginal'] = pd.to_numeric(df[col_monto], errors='coerce').fillna(0)
        df['PrecioSoles']    = df['PrecioOriginal']
        tc_hoy = get_tipo_cambio()
        df['PrecioDolares']  = (df['PrecioOriginal'] / tc_hoy).round(2)

    print(f"   -> FLUJO_CAJA procesado: {len(df):,} filas")
    print(f"   -> Columnas disponibles: {list(df.columns)}")
    return df


# ============================================================
# UPLOAD A GOOGLE SHEETS
# ============================================================

def _gsheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceCredentials.from_service_account_file(GSHEETS_CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def _clean_df(df):
    """Limpia NaN/inf para que Sheets los acepte."""
    def _c(x):
        if x is None: return ""
        try:
            if pd.isna(x): return ""
        except: pass
        if isinstance(x, float) and (x != x or abs(x) == float('inf')): return ""
        return str(x)
    return pd.concat([df[col].apply(_c) for col in df.columns], axis=1)


def upload_to_gsheets(dfs: dict):
    """
    dfs: {'VENTAS': df, 'STOCK': df, 'INGRESO_DEPOSITO': df, 'FLUJO_CAJA': df}
    """
    print("\n>> [GOOGLE SHEETS] Subiendo datos...")
    try:
        client = _gsheets_client()
        sp = client.open_by_key(GSHEETS_SPREADSHEET_ID)

        for tab_name, df in dfs.items():
            if df is None or len(df) == 0:
                print(f"   -> {tab_name}: sin datos, saltando")
                continue
            try:
                try: ws = sp.worksheet(tab_name); ws.clear()
                except: ws = sp.add_worksheet(title=tab_name, rows=len(df)+10, cols=len(df.columns)+5)
                df_clean = _clean_df(df)
                data = [df_clean.columns.tolist()] + df_clean.values.tolist()
                ws.update(data, value_input_option="RAW")
                print(f"   -> {tab_name}: {len(df):,} filas subidas")
            except Exception as e:
                print(f"   !! Error subiendo {tab_name}: {e}")

        print(f"   -> Dashboard: https://docs.google.com/spreadsheets/d/{GSHEETS_SPREADSHEET_ID}")
    except Exception as e:
        print(f"!! GSHEETS ERROR: {e}"); traceback.print_exc()


# ============================================================
# MAIN
# ============================================================

def main():
    print("="*70)
    print("   ETL FINANZAS-COMERCIAL — PADOVA SAC")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    # Limpiar stock temp
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx")):
        try: os.remove(f)
        except: pass

    driver = get_driver(DOWNLOAD_DIR)
    wait   = WebDriverWait(driver, 30)

    try:
        robust_login(driver, wait)

        execute_stock_extraction(driver, wait)

        # Cambiar dir de descarga para los reportes por año
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(DOWNLOAD_DIR)
        })

        execute_ventas_extraction(driver, wait)
        execute_ing_deposito_extraction(driver, wait)
        execute_flujo_caja_extraction(driver, wait)

    except Exception as e:
        print(f"!! CRITICAL ERROR: {e}"); traceback.print_exc()
    finally:
        driver.quit()

    # Transformar
    df_stock       = process_stock()
    df_ventas      = process_ventas(df_stock)
    df_ing_dep     = process_ing_deposito()
    df_flujo_caja  = process_flujo_caja()

    # Subir a Sheets
    upload_to_gsheets({
        "VENTAS":           df_ventas,
        "STOCK":            df_stock,
        "INGRESO_DEPOSITO": df_ing_dep,
        "FLUJO_CAJA":       df_flujo_caja,
    })

    print("\n" + "="*70)
    print("   PIPELINE COMPLETADO")
    print("="*70)


if __name__ == "__main__":
    main()
