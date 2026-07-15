"""
ETL Finanzas-Comercial — Padova SAC
Descarga 3 reportes de Evolta y los sube a Google Sheets:
  - VENTAS
  - STOCK
  - FLUJO_CAJA

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

# Ruta del cache persistente en disco (TC SUNAT venta con 2 decimales)
TC_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tc_cache.json')


def _cargar_cache_disco():
    """Carga el cache persistente de TC desde disco al iniciar el ETL."""
    global _TC_CACHE
    if not os.path.exists(TC_CACHE_FILE):
        print('   -> [TC] Cache en disco no encontrado, se creara en esta corrida')
        return
    try:
        with open(TC_CACHE_FILE, 'r', encoding='utf-8') as f:
            _TC_CACHE.update(json.load(f))
        print(f'   -> [TC] Cache cargado desde disco: {len(_TC_CACHE)} fechas')
    except Exception as e:
        print(f'   -> [TC] Error cargando cache disco: {e}')


def _guardar_cache_disco():
    """Guarda el cache de TC en disco al finalizar el ETL."""
    try:
        with open(TC_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(_TC_CACHE, f, ensure_ascii=False, indent=2)
        print(f'   -> [TC] Cache guardado en disco: {len(_TC_CACHE)} fechas')
    except Exception as e:
        print(f'   -> [TC] Error guardando cache disco: {e}')


def _fetch_tc_eapi(fecha_str):
    try:
        r = requests.get(f'https://free.e-api.net.pe/tipo-cambio/{fecha_str}.json', timeout=10)
        data = r.json()
        return float(data['venta']) if data.get('venta') else None
    except: return None


def _fetch_tc_bcrp(fecha_str):
    try:
        url = f'https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PD04637PD/json/{fecha_str}/{fecha_str}/ing'
        r = requests.get(url, timeout=10)
        data = json.loads(r.content.decode('utf-8-sig'))
        periodos = data.get('periods', [])
        if periodos and periodos[0].get('values'):
            return float(periodos[0]['values'][0])
    except: return None


def precargar_tc_fechas(fechas_set):
    """
    Pre-carga el cache con TC SUNAT venta (e-api) para las fechas dadas.
    Solo consulta fechas que NO esten ya en el cache (disco + memoria).
    Fallback a BCRP si e-api falla. Guarda en disco al terminar.
    """
    pendientes = sorted(f for f in fechas_set if f and f not in _TC_CACHE)
    if not pendientes:
        print(f'   -> [TC] Todas las fechas ya estan en cache ({len(_TC_CACHE)} total)')
        return
    print(f'   -> [TC] Consultando {len(pendientes)} fechas nuevas en SUNAT (e-api)...')
    ok = 0
    for fecha_str in pendientes:
        tc_raw = _fetch_tc_eapi(fecha_str)
        if tc_raw is None:
            tc_raw = _fetch_tc_bcrp(fecha_str)
        if tc_raw:
            _TC_CACHE[fecha_str] = round(tc_raw, 2)
            ok += 1
        time.sleep(0.15)
    _guardar_cache_disco()
    print(f'   -> [TC] {ok}/{len(pendientes)} TCs obtenidos y guardados')


def get_tipo_cambio(fecha=None):
    TC_RESPALDO = 3.75
    import pandas as _pd
    if fecha is None or (hasattr(_pd, 'isnull') and _pd.isnull(fecha)):
        fecha_dt = datetime.now()
    elif isinstance(fecha, str):
        try: fecha_dt = datetime.strptime(fecha[:10], '%Y-%m-%d')
        except: fecha_dt = datetime.now()
    elif hasattr(fecha, 'strftime'):
        try: fecha_dt = fecha.to_pydatetime() if hasattr(fecha, 'to_pydatetime') else fecha
        except: fecha_dt = datetime.now()
    else:
        fecha_dt = datetime.now()

    fecha_str = fecha_dt.strftime('%Y-%m-%d')
    if fecha_str in _TC_CACHE: return _TC_CACHE[fecha_str]

    # Buscar hasta 8 dias atras (fines de semana/feriados)
    for dias_atras in range(0, 8):
        f = (fecha_dt - timedelta(days=dias_atras)).strftime('%Y-%m-%d')
        if f in _TC_CACHE:
            _TC_CACHE[fecha_str] = _TC_CACHE[f]
            return _TC_CACHE[f]
        tc_raw = _fetch_tc_eapi(f) or _fetch_tc_bcrp(f)
        if tc_raw:
            tc = round(tc_raw, 2)
            print(f'   -> [TC] {fecha_str}: S/ {tc}')
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

URL_LOGIN              = "https://v4.evolta.pe/Login/Acceso/Index"
URL_REPORTE_STOCK      = "https://v4.evolta.pe/Reportes/RepCargaStock/IndexNuevoRepStock"
URL_REPORTE_VENTAS     = "https://v4.evolta.pe/Reportes/RepVenta/Index"
URL_REPORTE_FLUJO_CAJA = "https://v4.evolta.pe/Reportes/RepFlujoCaga/Index"

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900', 'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO', 'DOMINGO ORUE'
]

IS_CLOUD = os.name != 'nt'

if IS_CLOUD:
    DOWNLOAD_DIR_STOCK  = "/tmp/fc_stock"
    DOWNLOAD_DIR_VENTAS = "/tmp/fc_ventas"
    DOWNLOAD_DIR_FLUJO  = "/tmp/fc_flujo"
else:
    DOWNLOAD_DIR_STOCK  = r"C:\Users\MKT\Documents\EVOLTA\fc_stock"
    DOWNLOAD_DIR_VENTAS = r"C:\Users\MKT\Documents\EVOLTA\fc_ventas"
    DOWNLOAD_DIR_FLUJO  = r"C:\Users\MKT\Documents\EVOLTA\fc_flujo"

# ⬇ Cambia este ID por el del nuevo Google Sheet que crees para este dashboard
GSHEETS_SPREADSHEET_ID = os.environ.get("GSHEETS_SPREADSHEET_ID", "")

AÑOS = [2023, 2024, 2025, 2026]

for d in [DOWNLOAD_DIR_STOCK, DOWNLOAD_DIR_VENTAS, DOWNLOAD_DIR_FLUJO]:
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
    print(f"\n>> [STOCK] Descargando...")
    driver.get(URL_REPORTE_STOCK)
    time.sleep(3); dismiss_popup(driver)
    try:
        try: sel = wait.until(EC.presence_of_element_located((By.ID, "ProyectoId")))
        except: sel = driver.find_element(By.TAG_NAME, "select")
        try: Select(sel).select_by_visible_text("Todos")
        except:
            try: Select(sel).select_by_visible_text("TODOS")
            except: Select(sel).select_by_index(0)
        time.sleep(1)
    except Exception as e:
        print(f"   !! Warning selector: {e}")

    # Limpiar descargas previas antes de medir nuevas
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_STOCK, "*.xlsx")):
        try: os.remove(f)
        except: pass

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR_STOCK, "*.*")))
    export_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnExportar")))
    driver.execute_script("arguments[0].click();", export_btn)

    archivo = esperar_descarga_nueva(DOWNLOAD_DIR_STOCK, existing, timeout=480)
    if not archivo:
        print("   !! No se descargó stock"); return None
    dest = os.path.join(DOWNLOAD_DIR_STOCK, "ReporteStock.xlsx")
    _mover_descarga(archivo, dest)
    return dest


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
    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR_VENTAS, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR_VENTAS, existing, timeout=120)
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
# EXTRACCIÓN — FLUJO DE CAJA (por año)
# ============================================================

def _set_filtros_flujo_caja(driver):
    """
    Configura los dropdowns del reporte Flujo de Caja en Evolta:
    Proyecto=TODOS, Etapa=TODOS, Año Ini=2023/Enero, Año Fin=actual/mes actual.
    """
    now   = datetime.now()
    MESES = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
             'Julio','Agosto','Setiembre','Octubre','Noviembre','Diciembre']
    año_fin = str(now.year)
    mes_fin = MESES[now.month - 1]

    selects  = driver.find_elements(By.TAG_NAME, "select")
    año_sels, mes_sels = [], []

    for sel in selects:
        opts   = [o.text.strip() for o in sel.find_elements(By.TAG_NAME, "option")]
        opts_u = [o.upper() for o in opts]
        if any(str(y) in opts for y in range(2020, 2030)):
            año_sels.append(sel)
        elif any(m in opts for m in MESES):
            mes_sels.append(sel)
        elif 'TODOS' in opts_u or 'TODO' in opts_u or 'ETAPA COMERCIAL' in opts_u:
            for txt in ['TODOS', 'Todos', 'Todo']:
                try: Select(sel).select_by_visible_text(txt); time.sleep(0.3); break
                except: pass

    if año_sels:
        try: Select(año_sels[0]).select_by_visible_text('2023'); time.sleep(0.3)
        except: pass
    if len(año_sels) >= 2:
        try: Select(año_sels[1]).select_by_visible_text(año_fin); time.sleep(0.3)
        except: pass
    if mes_sels:
        try: Select(mes_sels[0]).select_by_visible_text('Enero'); time.sleep(0.3)
        except: pass
    if len(mes_sels) >= 2:
        try: Select(mes_sels[1]).select_by_visible_text(mes_fin); time.sleep(0.3)
        except: pass

    print(f"   -> Filtros: Enero 2023 - {mes_fin} {año_fin}")


def execute_flujo_caja_extraction(driver, wait):
    """Descarga unica del reporte Flujo de Caja con rango 2023 - hoy."""
    print("\n" + "="*60)
    print(">> [FLUJO_CAJA] Descarga unica (Ene 2023 - hoy)")
    for f in glob.glob(os.path.join(DOWNLOAD_DIR_FLUJO, "*.*")):
        try: os.remove(f)
        except: pass

    driver.get(URL_REPORTE_FLUJO_CAJA)
    time.sleep(4); dismiss_popup(driver)
    _set_filtros_flujo_caja(driver)

    existing = set(glob.glob(os.path.join(DOWNLOAD_DIR_FLUJO, "*.*")))
    _click_exportar(driver, wait)
    time.sleep(5)
    archivo = esperar_descarga_nueva(DOWNLOAD_DIR_FLUJO, existing, timeout=480)
    if archivo:
        ext  = os.path.splitext(archivo)[1].lower()
        dest = os.path.join(DOWNLOAD_DIR_FLUJO, f"ReporteFlujoCajaTotal{ext}")
        _mover_descarga(archivo, dest)
    else:
        print("   !! No se descargo flujo_caja")
        driver.save_screenshot(os.path.join(DOWNLOAD_DIR_FLUJO, "debug_flujo_total.png"))


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
# NORMALIZACIÓN — Evolta renombró columnas (jul-2026):
#   VENTAS: TipoMoneda -> Moneda_Base/Moneda_OC
#           stubs PrecioBase_N/PrecioLista_N/DescuentoLista_N/TotalLista_N ->
#           Precio_Base_N/PrecioLista_OC_N/DescuentoLista_OC_N/TotalVenta_OC_N,
#           y el reporte ahora ya viene 1 fila = 1 unidad (T/M, TipoInmueble,
#           NroInmueble, N_Unidad, etc. sin sufijo _N).
#   STOCK:  Moneda/PrecioVenta -> Moneda_Base/PrecioBase, Moneda_Lista/PrecioLista,
#           Moneda_OC/PrecioVenta_OC.
# ============================================================

def _normalizar_moneda_ventas(df):
    """Crea TipoMoneda a partir de Moneda_OC (prioridad) o Moneda_Base si el
    reporte ya no trae la columna TipoMoneda tal cual."""
    if 'TipoMoneda' in df.columns:
        return df
    if 'Moneda_OC' not in df.columns and 'Moneda_Base' not in df.columns:
        return df
    df = df.copy()
    moneda_oc   = df['Moneda_OC'].astype(str).str.strip() if 'Moneda_OC' in df.columns else pd.Series('', index=df.index)
    moneda_base = df['Moneda_Base'].astype(str).str.strip() if 'Moneda_Base' in df.columns else pd.Series('', index=df.index)
    df['TipoMoneda'] = moneda_oc.where(moneda_oc != '', moneda_base)
    print('   -> [MONEDA] TipoMoneda derivado de Moneda_OC/Moneda_Base')
    return df


def _normalizar_precio_moneda_stock(df):
    """Crea PrecioVenta/Moneda a partir de las columnas nuevas de Evolta cuando
    el reporte de stock ya no trae 'PrecioVenta'/'Moneda' tal cual.
    Prioridad: PrecioVenta_OC/Moneda_OC (precio real de la OC, si la unidad ya
    tiene una) -> PrecioLista/Moneda_Lista (precio de lista, unidades
    disponibles sin OC) -> PrecioBase/Moneda_Base."""
    if 'PrecioVenta' in df.columns and 'Moneda' in df.columns:
        return df
    df = df.copy()

    def _num(col):
        return pd.to_numeric(df[col], errors='coerce').fillna(0) if col in df.columns else pd.Series(0.0, index=df.index)

    def _str(col):
        return df[col].astype(str).str.strip() if col in df.columns else pd.Series('', index=df.index)

    precio_oc, precio_lista, precio_base = _num('PrecioVenta_OC'), _num('PrecioLista'), _num('PrecioBase')
    moneda_oc, moneda_lista, moneda_base = _str('Moneda_OC'), _str('Moneda_Lista'), _str('Moneda_Base')

    usar_oc    = precio_oc > 0
    usar_lista = ~usar_oc & (precio_lista > 0)

    df['PrecioVenta'] = precio_oc.where(usar_oc, precio_lista.where(usar_lista, precio_base))
    df['Moneda'] = moneda_oc.where(usar_oc & (moneda_oc != ''),
                       moneda_lista.where(usar_lista & (moneda_lista != ''), moneda_base))
    print('   -> [MONEDA] PrecioVenta/Moneda derivados de PrecioVenta_OC/PrecioLista/PrecioBase')
    return df


def _marcar_comercio(df):
    """Modelo=COMERCIO -> TipoInmueble='Comercio' (separa locales comerciales de depas)."""
    if 'Modelo' not in df.columns or 'TipoInmueble' not in df.columns:
        return df
    df = df.copy()
    mask = df['Modelo'].astype(str).str.upper().str.strip() == 'COMERCIO'
    if mask.any():
        df.loc[mask, 'TipoInmueble'] = 'Comercio'
        print(f'   -> [UNPIVOT] {mask.sum()} unidades COMERCIO separadas')
    return df


def _seleccionar_slot_por_n_unidad(df):
    """Formato nuevo Evolta: el reporte ya viene 1 fila = 1 unidad (TipoInmueble,
    NroInmueble, N_Unidad, etc. son columnas planas), pero los montos siguen
    anchos por slot (Precio_Base_N / PrecioLista_OC_N / DescuentoLista_OC_N /
    TotalVenta_OC_N, N=1..6). Selecciona el slot que corresponde a N_Unidad
    de cada fila para armar PrecioBase/PrecioLista/DescuentoLista/PrecioVenta."""
    df = df.copy()
    slot_map = {
        'PrecioBase':     'Precio_Base',
        'PrecioLista':    'PrecioLista_OC',
        'DescuentoLista': 'DescuentoLista_OC',
        'PrecioVenta':    'TotalVenta_OC',
    }
    n_col = pd.to_numeric(df['N_Unidad'], errors='coerce').fillna(1).astype(int).clip(1, 6)
    for destino, prefijo in slot_map.items():
        cols_disponibles = {n: f'{prefijo}_{n}' for n in range(1, 7) if f'{prefijo}_{n}' in df.columns}
        if not cols_disponibles:
            continue
        valores = pd.Series(0.0, index=df.index)
        for n, col_slot in cols_disponibles.items():
            m = n_col == n
            valores.loc[m] = pd.to_numeric(df.loc[m, col_slot], errors='coerce').fillna(0)
        df[destino] = valores
    df = _marcar_comercio(df)
    print(f'   -> [UNPIVOT] Formato nuevo detectado (1 fila=1 unidad), montos armados via N_Unidad')
    return df


# ============================================================
# UNPIVOT VENTAS: wide (1 fila=transacción) → long (1 fila=unidad)
# ============================================================

def _unpivot_ventas(df):
    """Convierte ventas de formato ancho a largo: una fila por unidad (loop manual)."""
    import re as _re
    stubs = ['T/M', 'TipoInmueble', 'Modelo', 'NroInmueble', 'NroPiso', 'Vista',
             'PrecioBase', 'PrecioLista', 'DescuentoLista', 'TotalLista', 'PrioridadOC', 'Orden']

    # Detectar que numeros de unidad existen (busca cualquier stub_N en las columnas)
    nums = sorted(set(
        int(m.group(1))
        for col in df.columns
        for m in [_re.search(r'_([0-9]+)$', col)]
        if m and any(col == f'{s}_{m.group(1)}' for s in stubs)
    ))

    if not nums:
        if 'N_Unidad' in df.columns:
            return _seleccionar_slot_por_n_unidad(df)
        print('   -> [UNPIVOT] Sin columnas Stub_N detectadas, retornando sin cambios')
        return df

    # Columnas fijas: todo lo que NO sea stub_N
    stub_cols_all = {f'{s}_{n}' for s in stubs for n in nums}
    cols_fijas = [c for c in df.columns if c not in stub_cols_all]

    partes = []
    for n in nums:
        cols_n = {s: f'{s}_{n}' for s in stubs if f'{s}_{n}' in df.columns}
        if not cols_n:
            continue
        parte = df[cols_fijas + list(cols_n.values())].copy()
        parte = parte.rename(columns={v: k for k, v in cols_n.items()})
        parte['N_Unidad'] = n
        partes.append(parte)

    if not partes:
        print('   -> [UNPIVOT] Sin partes generadas, retornando sin cambios')
        return df

    df_long = pd.concat(partes, ignore_index=True)

    # Filtrar filas sin TipoInmueble (slots vacios de la transaccion)
    if 'TipoInmueble' in df_long.columns:
        df_long = df_long[
            df_long['TipoInmueble'].notna() &
            (df_long['TipoInmueble'].astype(str).str.strip() != '') &
            (df_long['TipoInmueble'].astype(str).str.strip().str.lower() != 'nan')
        ]

    # Modelo=COMERCIO -> TipoInmueble='Comercio' (separa locales comerciales de depas)
    if 'Modelo' in df_long.columns:
        mask = df_long['Modelo'].astype(str).str.upper().str.strip() == 'COMERCIO'
        if mask.any():
            df_long.loc[mask, 'TipoInmueble'] = 'Comercio'
            print(f'   -> [UNPIVOT] {mask.sum()} unidades COMERCIO separadas')

    print(f'   -> [UNPIVOT] {len(df):,} transacciones -> {len(df_long):,} unidades')
    return df_long


def _corregir_moneda_sunny(df):
    """Sunny: precios < 600k marcados como SOLES son en realidad USD."""
    if not {'Proyecto', 'TipoMoneda'}.issubset(df.columns): return df
    col_p = next((c for c in ['TotalLista', 'PrecioVenta'] if c in df.columns), None)
    if not col_p: return df
    df = df.copy()
    precios = pd.to_numeric(df[col_p], errors='coerce').fillna(0)
    mask = (df['Proyecto'].str.upper().str.contains('SUNNY', na=False) &
            ~df['TipoMoneda'].str.upper().str.contains('DOLAR|USD', na=False) &
            precios.between(1, 599_999))
    n = mask.sum()
    if n:
        df.loc[mask, 'TipoMoneda'] = 'DOLAR'
        print(f"   -> [MONEDA] Sunny: {n} unidades →DOLAR")
    return df


def _corregir_moneda_litoral(df):
    """Litoral: comercio=siempre USD, estac/dep por rango de precio."""
    if not {'Proyecto', 'TipoMoneda', 'TipoInmueble'}.issubset(df.columns): return df
    col_p = next((c for c in ['TotalLista', 'PrecioVenta'] if c in df.columns), None)
    if not col_p: return df
    df = df.copy()
    precios = pd.to_numeric(df[col_p], errors='coerce').fillna(0)
    lit  = df['Proyecto'].str.upper().str.contains('LITORAL', na=False)
    tipo = df['TipoInmueble'].astype(str).str.upper().str.strip()
    # Comercio: siempre USD
    mask_c = lit & tipo.str.contains('COMERCI|LOCAL', na=False)
    if 'Modelo' in df.columns:
        mask_c = mask_c | (lit & (df['Modelo'].astype(str).str.upper().str.strip() == 'COMERCIO'))
    n_c = mask_c.sum()
    if n_c:
        df.loc[mask_c, 'TipoMoneda'] = 'DOLAR'
        print(f"   -> [MONEDA] Litoral Comercio: {n_c} →DOLAR")
    # Estacionamiento: 10k–29k = USD, fuera = SOLES
    mask_e = lit & tipo.str.contains('ESTACION', na=False) & ~tipo.str.contains('BICICLET|COMERCI|LOCAL', na=False)
    df.loc[mask_e & precios.between(10_000, 29_000), 'TipoMoneda'] = 'DOLAR'
    df.loc[mask_e & ~precios.between(10_000, 29_000), 'TipoMoneda'] = 'SOLES'
    # Depósito: 1700–3300 = USD, fuera = SOLES
    mask_d = lit & (tipo.str.contains('DEPOSIT', na=False) | tipo.str.contains('DEPÓSIT', na=False)) & ~tipo.str.contains('COMERCI', na=False)
    df.loc[mask_d & precios.between(1_700, 3_300), 'TipoMoneda'] = 'DOLAR'
    df.loc[mask_d & ~precios.between(1_700, 3_300), 'TipoMoneda'] = 'SOLES'
    print(f"   -> [MONEDA] Litoral Estac/Dep: correcciones aplicadas")
    return df


# ============================================================
# CORRECCIÓN MONEDA (stock como fuente de verdad)
# ============================================================

def corregir_moneda_con_stock(df_ventas, df_stock):
    """Corrige TipoMoneda en ventas usando el stock como referencia.
    Evolta a veces exporta la moneda equivocada en el reporte de ventas."""
    if df_stock is None or len(df_stock) == 0: return df_ventas
    df_stock = df_stock.copy()
    df_stock.columns = df_stock.columns.str.strip()
    col_proy_s   = next((c for c in df_stock.columns if c.strip() == 'Proyecto'), None)
    col_nro_s    = next((c for c in df_stock.columns if c.strip() == 'NroInmuebleActual'), None) \
                or next((c for c in df_stock.columns if 'NroInmueble' in c), None)
    col_moneda_s = next((c for c in df_stock.columns if c.strip() == 'Moneda'), None)
    if not col_proy_s or not col_nro_s or not col_moneda_s: return df_ventas

    def norm_nro(v):
        s = str(v).strip()
        if s.endswith('.0'): s = s[:-2]
        return s.upper()

    lookup = {}
    for _, row in df_stock.iterrows():
        proy = str(row[col_proy_s]).strip().upper()
        nro  = norm_nro(row[col_nro_s])
        mon  = str(row[col_moneda_s]).strip().upper()
        if proy and nro and nro not in ('', 'NAN', 'NONE'):
            lookup[(proy, nro)] = mon
    print(f"   -> [MONEDA] Lookup stock: {len(lookup)} unidades")

    col_proy_v   = 'Proyecto'   if 'Proyecto'   in df_ventas.columns else None
    col_nro_v    = 'NroInmueble' if 'NroInmueble' in df_ventas.columns else None
    col_moneda_v = 'TipoMoneda' if 'TipoMoneda'  in df_ventas.columns else None
    if not col_proy_v or not col_nro_v or not col_moneda_v: return df_ventas

    df_ventas = df_ventas.copy()
    corregidos = 0
    for idx, row in df_ventas.iterrows():
        moneda_v = str(row[col_moneda_v]).upper().strip()
        if 'DOLAR' not in moneda_v and 'USD' not in moneda_v: continue
        proy_v = str(row[col_proy_v]).strip().upper()
        nro_v  = norm_nro(row[col_nro_v])
        moneda_stock = lookup.get((proy_v, nro_v))
        if moneda_stock and 'DOLAR' not in moneda_stock and 'USD' not in moneda_stock:
            df_ventas.at[idx, col_moneda_v] = moneda_stock
            corregidos += 1
    print(f"   -> [MONEDA] Corregidos: {corregidos} registros")
    return df_ventas


# ============================================================
# TRANSFORMACIÓN — STOCK
# ============================================================

def process_stock():
    print("\n>> [TRANSFORM STOCK]")
    archivos = glob.glob(os.path.join(DOWNLOAD_DIR_STOCK, "*.xlsx"))
    if not archivos: return None
    df = pd.read_excel(max(archivos, key=os.path.getctime))
    df.columns = df.columns.str.strip()
    df = _filtrar_proyectos(df)
    df = _normalizar_precio_moneda_stock(df)

    col_precio = next((c for c in ['PrecioVenta', 'PrecioLista'] if c in df.columns), None)
    col_moneda = 'Moneda' if 'Moneda' in df.columns else None
    col_fecha  = next((c for c in ['FechaSepDefinitiva', 'FechaVenta'] if c in df.columns), None)

    if col_precio and col_moneda:
        df = convertir_monedas(df, col_precio, col_moneda, col_fecha)

    print(f"   -> STOCK procesado: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — VENTAS
# ============================================================

def process_ventas(df_stock_crudo=None):
    print("\n>> [TRANSFORM VENTAS]")
    df = _leer_por_año(DOWNLOAD_DIR_VENTAS, "ReporteVenta", AÑOS)
    if df is None: return None
    df = _filtrar_proyectos(df)
    df = _normalizar_moneda_ventas(df)

    # Unpivot: wide (1 fila=transacción) → long (1 fila=unidad)
    df = _unpivot_ventas(df)

    # Precio por unidad: TotalLista es el precio individual de cada ítem
    if 'TotalLista' in df.columns:
        df['PrecioVenta'] = pd.to_numeric(df['TotalLista'], errors='coerce').fillna(0)

    col_fecha  = next((c for c in ['FechaVenta', 'FechaEntrega_Minuta'] if c in df.columns), None)
    col_moneda = 'TipoMoneda' if 'TipoMoneda' in df.columns else None

    # Pre-cargar TC SUNAT venta para fechas unicas del dataset (solo las nuevas)
    if col_fecha and col_fecha in df.columns:
        fechas_unicas = set(
            str(f)[:10] for f in df[col_fecha].dropna()
            if str(f).strip() not in ('', 'nan', 'NaT')
        )
        precargar_tc_fechas(fechas_unicas)

    # Corregir moneda: stock lookup -> Sunny -> Litoral
    if df_stock_crudo is not None and col_moneda:
        df = corregir_moneda_con_stock(df, df_stock_crudo)
    df = _corregir_moneda_sunny(df)
    df = _corregir_moneda_litoral(df)

    # Convertir a soles/dolares con TC SUNAT venta por fecha de venta
    if 'PrecioVenta' in df.columns and col_moneda:
        df = convertir_monedas(df, 'PrecioVenta', col_moneda, col_fecha)

    print(f"   -> VENTAS procesadas: {len(df):,} filas")
    return df


# ============================================================
# TRANSFORMACIÓN — FLUJO DE CAJA
# ============================================================

def process_flujo_caja():
    """
    Lee y procesa el reporte de Flujo de Caja.
    NOTA: Las columnas exactas se conocerán cuando Evolta exporte
    el reporte por primera vez. Esta función las detecta dinámicamente.
    Ajustar col_monto / col_moneda / col_fecha una vez que se vea el Excel.
    """
    print("\n>> [TRANSFORM FLUJO_CAJA]")
    df = None
    for ext in ['.csv', '.xlsx']:
        ruta = os.path.join(DOWNLOAD_DIR_FLUJO, f"ReporteFlujoCajaTotal{ext}")
        if not os.path.exists(ruta):
            continue
        try:
            df = pd.read_csv(ruta, encoding='utf-8', low_memory=False) if ext == '.csv' else pd.read_excel(ruta)
            print(f"   -> ReporteFlujoCajaTotal: {len(df):,} filas")
            break
        except Exception as e:
            print(f"   !! Error leyendo {ruta}: {e}")
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
    dfs: {'VENTAS': df, 'STOCK': df, 'FLUJO_CAJA': df}
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

    # Cargar cache TC desde disco (e-api SUNAT venta, 2 decimales)
    _cargar_cache_disco()

    driver = get_driver(DOWNLOAD_DIR_STOCK)
    wait   = WebDriverWait(driver, 30)

    try:
        robust_login(driver, wait)

        execute_stock_extraction(driver, wait)

        # Cambiar dir de descarga a ventas antes de extraer ventas
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(DOWNLOAD_DIR_VENTAS)
        })
        execute_ventas_extraction(driver, wait)

        # Cambiar dir de descarga a flujo antes de extraer flujo
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": os.path.abspath(DOWNLOAD_DIR_FLUJO)
        })
        execute_flujo_caja_extraction(driver, wait)

    except Exception as e:
        print(f"!! CRITICAL ERROR: {e}"); traceback.print_exc()
    finally:
        driver.quit()

    # Leer stock crudo para corregir moneda en ventas
    df_stock_crudo = None
    try:
        archivos = glob.glob(os.path.join(DOWNLOAD_DIR_STOCK, "*.xlsx"))
        if archivos:
            df_stock_crudo = pd.read_excel(max(archivos, key=os.path.getctime))
            df_stock_crudo.columns = df_stock_crudo.columns.str.strip()
            df_stock_crudo = _normalizar_precio_moneda_stock(df_stock_crudo)
            print(f"\n>> [MONEDA] Stock crudo cargado: {len(df_stock_crudo):,} filas")
    except Exception as e:
        print(f"!! Warning stock crudo: {e}")

    # Transformar
    df_stock      = process_stock()
    df_ventas     = process_ventas(df_stock_crudo)
    df_flujo_caja = process_flujo_caja()

    # Subir a Sheets
    upload_to_gsheets({
        "VENTAS":     df_ventas,
        "STOCK":      df_stock,
        "FLUJO_CAJA": df_flujo_caja,
    })

    print("\n" + "="*70)
    print("   PIPELINE COMPLETADO")
    print("="*70)


if __name__ == "__main__":
    main()
