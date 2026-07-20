"""
ETL Finanzas-Comercial — Padova SAC
Descarga 3 reportes de Evolta y los sube a SharePoint/OneDrive corporativo:
  - VENTAS
  - STOCK
  - FLUJO_CAJA

Basado en ETL_Padova_MultiRol.py. Reutiliza misma lógica de
tipo de cambio; la extraccion ahora es directa via API (sin
Selenium/navegador, ver evolta_client.py) y el upload va a un
Excel en SharePoint en vez de Google Sheets (ver sharepoint_client.py).
"""

import time, os, json, re, tempfile, shutil, requests, traceback
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from sharepoint_client import SharePointClient
from evolta_client import EvoltaDirectClient, leer_excel_bytes

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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


# Fuente oficial SUNAT (misma API validada en tc-service contra el Master de
# Ventas real: devuelve compra/venta correctos y ya incluye el arrastre de
# fin de semana de SUNAT). Rate limit observado: 429 tras ~3 llamadas rapidas,
# por eso el espaciado de 1.5s entre consultas en precargar_tc_fechas.
SUNAT_API_BASE = 'https://api.apis.net.pe/v1/tipo-cambio-sunat'
SUNAT_REQUEST_DELAY_SECONDS = 1.5


def _fetch_tc_sunat_oficial(fecha_str):
    try:
        r = requests.get(SUNAT_API_BASE, params={'fecha': fecha_str}, timeout=10)
        if r.status_code == 429:
            return None
        r.raise_for_status()
        data = r.json()
        return float(data['venta']) if data.get('venta') else None
    except: return None


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
    Pre-carga el cache con TC SUNAT venta (fuente oficial api.apis.net.pe)
    para las fechas dadas. Solo consulta fechas que NO esten ya en el cache
    (disco + memoria). Fallback a e-api y luego a BCRP si la oficial falla
    (caida puntual o 429). Guarda en disco al terminar.
    """
    pendientes = sorted(f for f in fechas_set if f and f not in _TC_CACHE)
    if not pendientes:
        print(f'   -> [TC] Todas las fechas ya estan en cache ({len(_TC_CACHE)} total)')
        return
    print(f'   -> [TC] Consultando {len(pendientes)} fechas nuevas en SUNAT (oficial)...')
    ok = 0
    for fecha_str in pendientes:
        tc_raw = _fetch_tc_sunat_oficial(fecha_str)
        if tc_raw is None:
            tc_raw = _fetch_tc_eapi(fecha_str)
        if tc_raw is None:
            tc_raw = _fetch_tc_bcrp(fecha_str)
        if tc_raw:
            _TC_CACHE[fecha_str] = round(tc_raw, 2)
            ok += 1
        time.sleep(SUNAT_REQUEST_DELAY_SECONDS)
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

EVOLTA_USER = os.environ.get("EVOLTA_USER", "calopez")
EVOLTA_PASS = os.environ.get("EVOLTA_PASS", "")

TARGET_PROJECTS = [
    'SUNNY', 'LITORAL 900', 'HELIO - SANTA BEATRIZ',
    'LOMAS DE CARABAYLLO', 'DOMINGO ORUE'
]

# Rango de fechas para Ventas y Flujo de Caja. Antes se pedia por año via
# Selenium (un click por año); la API directa acepta el rango completo en
# una sola llamada (confirmado 2026-07-20: 2023-hoy -> 1,163 filas / 14s).
FECHA_INICIO_REPORTES = "01/01/2023"
ANIO_INICIO_FLUJO_CAJA = "2023"
MES_INICIO_FLUJO_CAJA = "1"

# Carpeta y nombre del Excel en SharePoint (mismo patron que tc-service)
SHAREPOINT_DASHBOARD_FOLDER = os.environ.get(
    "SHAREPOINT_DASHBOARD_FOLDER",
    "FINANZAS/TI/AUTOMATIZACIONES/DASHBOARD_FINANZAS_COMERCIAL",
)
SHAREPOINT_DASHBOARD_FILENAME = os.environ.get(
    "SHAREPOINT_DASHBOARD_FILENAME", "Dashboard_Finanzas_Comercial.xlsx"
)


# ============================================================
# EXTRACCIÓN — EVOLTA DIRECTO (sin navegador)
# ============================================================

# Espaciado entre los 3 reportes contra apiwebcr.evoltacomunica.com. Al
# probar esto por primera vez (2026-07-20), pedir los 3 reportes casi sin
# pausa (ritmo de maquina) hizo fallar de forma intermitente cualquiera de
# ellos que quedara primero (400 / timeout / conexion reiniciada) -- una
# persona navegando manualmente por las 3 pantallas nunca tuvo ese problema,
# porque entre cada clic pasan varios segundos. Mismo sintoma y misma
# solucion que ya usamos con el 429 de la API de SUNAT (ver
# SUNAT_REQUEST_DELAY_SECONDS): espaciar las llamadas en vez de pelear
# contra el limite.
EVOLTA_REPORT_DELAY_SECONDS = 5


def extraer_reportes_evolta():
    """Login + descarga de los 3 reportes via API directa de EVOLTA
    (evolta_client.py). Devuelve dict {'stock', 'ventas', 'flujo_caja'}
    con DataFrames crudos (cualquiera puede ser None si Evolta fallo)."""
    print("\n>> [EVOLTA] Login directo...")
    client = EvoltaDirectClient(EVOLTA_USER, EVOLTA_PASS)
    client.login()
    print(">> [EVOLTA] Login OK")

    client.calentar_conexion_reportes()

    hoy = datetime.now().strftime("%d/%m/%Y")
    resultado = {}

    print("\n>> [VENTAS] Descargando...")
    try:
        resultado['ventas'] = leer_excel_bytes(client.export_ventas(FECHA_INICIO_REPORTES, hoy))
        print(f"   -> VENTAS: {len(resultado['ventas']):,} filas")
    except Exception as e:
        print(f"   !! Error VENTAS: {e}")
        resultado['ventas'] = None

    time.sleep(EVOLTA_REPORT_DELAY_SECONDS)
    print("\n>> [STOCK] Descargando...")
    try:
        resultado['stock'] = leer_excel_bytes(client.export_stock())
        print(f"   -> STOCK: {len(resultado['stock']):,} filas")
    except Exception as e:
        print(f"   !! Error STOCK: {e}")
        resultado['stock'] = None

    time.sleep(EVOLTA_REPORT_DELAY_SECONDS)
    print("\n>> [FLUJO_CAJA] Descargando...")
    try:
        contenido = client.export_flujo_caja(
            ANIO_INICIO_FLUJO_CAJA, MES_INICIO_FLUJO_CAJA,
            str(datetime.now().year), str(datetime.now().month),
        )
        resultado['flujo_caja'] = leer_excel_bytes(contenido)
        print(f"   -> FLUJO_CAJA: {len(resultado['flujo_caja']):,} filas")
    except Exception as e:
        print(f"   !! Error FLUJO_CAJA: {e}")
        resultado['flujo_caja'] = None

    return resultado


def _filtrar_proyectos(df, col='Proyecto'):
    if col in df.columns:
        return df[df[col].str.upper().isin(TARGET_PROJECTS)]
    return df


# ============================================================
# NORMALIZACIÓN — Evolta renombró columnas (jul-2026):
#   VENTAS: sigue siendo ancho (1 fila = 1 transacción, stubs _N por
#           inmueble). Solo cambiaron los prefijos: TipoMoneda -> Moneda_OC,
#           stubs PrecioBase_N/PrecioLista_N/DescuentoLista_N/TotalLista_N ->
#           Precio_Base_N/PrecioLista_OC_N/DescuentoLista_OC_N/TotalVenta_OC_N.
#           Se traducen a los nombres viejos (_normalizar_moneda_ventas) y el
#           unpivot de siempre (_unpivot_ventas) sigue funcionando igual.
#   STOCK:  Moneda/PrecioVenta -> Moneda_Base/PrecioBase, Moneda_Lista/PrecioLista,
#           Moneda_OC/PrecioVenta_OC.
# ============================================================

_RENAME_VENTAS_GLOBAL = {
    'PrecioVenta_OC':        'PrecioVenta',
    'MontoDescuento_OC':     'MontoDescuento',
    'MontoSeparacion_OC':    'MontoSeparacion',
    'BonoVerde_OC':          'BonoVerde',
    'MontoBono_OC':          'MontoBono',
    'MontoPagadoBono_OC':    'MontoPagadoBono',
    'MontoCuotaInicial_OC':  'MontoCuotaInicial',
    'MontoPagadoCI_OC':      'MontoPagadoCI',
    'MontoFinanciamiento_OC':'MontoFinanciamiento',
    'MontoDesembolsado_OC':  'MontoDesembolsado',
    'Moneda_OC':             'TipoMoneda',
}

_RENAME_VENTAS_INMUEBLE_PATTERNS = [
    (re.compile(r'^Precio_Base_(\d+)$'),       'PrecioBase_{}'),
    (re.compile(r'^PrecioLista_OC_(\d+)$'),    'PrecioLista_{}'),
    (re.compile(r'^DescuentoLista_OC_(\d+)$'), 'DescuentoLista_{}'),
    (re.compile(r'^TotalVenta_OC_(\d+)$'),     'TotalLista_{}'),
]


def _normalizar_moneda_ventas(df):
    """Traduce headers nuevos de Evolta (ventas) a los nombres internos
    viejos: PrecioVenta_OC->PrecioVenta, Moneda_OC->TipoMoneda, y los stubs
    por inmueble Precio_Base_N/PrecioLista_OC_N/DescuentoLista_OC_N/
    TotalVenta_OC_N -> PrecioBase_N/PrecioLista_N/DescuentoLista_N/TotalLista_N.
    Si el reporte viene en formato viejo, ninguna de estas columnas existe y
    el rename queda vacio (no-op)."""
    rename_map = {k: v for k, v in _RENAME_VENTAS_GLOBAL.items() if k in df.columns}
    for col in df.columns:
        for pattern, target in _RENAME_VENTAS_INMUEBLE_PATTERNS:
            m = pattern.match(col)
            if m:
                rename_map[col] = target.format(m.group(1))
                break
    if not rename_map:
        return df
    print(f'   -> [HEADERS] Ventas: renombrando {len(rename_map)} columna(s) al formato interno')
    df = df.rename(columns=rename_map)
    # Fallback si Moneda_OC no existia (unidad sin OC todavia) pero si Moneda_Base
    if 'TipoMoneda' not in df.columns and 'Moneda_Base' in df.columns:
        df = df.rename(columns={'Moneda_Base': 'TipoMoneda'})
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

def process_stock(df):
    print("\n>> [TRANSFORM STOCK]")
    if df is None: return None
    df = df.copy()
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

def process_ventas(df, df_stock_crudo=None):
    print("\n>> [TRANSFORM VENTAS]")
    if df is None: return None
    df = df.copy()
    df = _filtrar_proyectos(df)
    df = _normalizar_moneda_ventas(df)

    # Unpivot: wide (1 fila=transacción) → long (1 fila=unidad)
    df = _unpivot_ventas(df)

    # Precio por unidad: TotalLista es el precio individual de cada item.
    # PrecioVenta (columna fija) es el total de TODO el combo/transaccion,
    # duplicado en cada fila del unpivot -> siempre hay que pisarlo con
    # TotalLista, que es el precio correcto por unidad.
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

def process_flujo_caja(df):
    """Procesa el reporte de Flujo de Caja. Detecta las columnas de
    monto/moneda/fecha dinamicamente (el nombre exacto puede variar)."""
    print("\n>> [TRANSFORM FLUJO_CAJA]")
    if df is None:
        print("   !! Sin datos de flujo de caja todavía")
        return None
    df = df.copy()
    df = _filtrar_proyectos(df)

    # Detectar columnas dinámicamente
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
# UPLOAD A SHAREPOINT/ONEDRIVE CORPORATIVO
# ============================================================

def _clean_df(df):
    """Limpia NaN/inf para que el Excel no falle al escribir."""
    def _c(x):
        if x is None: return ""
        try:
            if pd.isna(x): return ""
        except: pass
        if isinstance(x, float) and (x != x or abs(x) == float('inf')): return ""
        return x
    return pd.concat([df[col].apply(_c) for col in df.columns], axis=1)


def upload_to_sharepoint(dfs: dict):
    """
    dfs: {'VENTAS': df, 'STOCK': df, 'FLUJO_CAJA': df}
    Arma un solo Excel (una hoja por tab) y lo sube a la carpeta corporativa
    de SharePoint, reemplazando el archivo anterior (mismo nombre).
    """
    print("\n>> [SHAREPOINT] Subiendo datos...")
    tmp_dir = tempfile.mkdtemp(prefix="dashboard_fc_")
    local_path = Path(tmp_dir) / SHAREPOINT_DASHBOARD_FILENAME
    try:
        with pd.ExcelWriter(local_path, engine="openpyxl") as writer:
            for tab_name, df in dfs.items():
                if df is None or len(df) == 0:
                    print(f"   -> {tab_name}: sin datos, saltando")
                    continue
                _clean_df(df).to_excel(writer, sheet_name=tab_name, index=False)
                print(f"   -> {tab_name}: {len(df):,} filas listas")

        client = SharePointClient()
        client.upload_file(local_path, SHAREPOINT_DASHBOARD_FOLDER)
        print(f"   -> Subido a SharePoint: {SHAREPOINT_DASHBOARD_FOLDER}/{SHAREPOINT_DASHBOARD_FILENAME}")
    except Exception as e:
        print(f"!! SHAREPOINT ERROR: {e}"); traceback.print_exc()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


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

    crudos = extraer_reportes_evolta()

    # Stock crudo (columnas normalizadas) para el lookup de moneda en ventas
    df_stock_crudo = None
    if crudos['stock'] is not None:
        df_stock_crudo = crudos['stock'].copy()
        df_stock_crudo.columns = df_stock_crudo.columns.str.strip()
        df_stock_crudo = _normalizar_precio_moneda_stock(df_stock_crudo)

    # Transformar
    df_stock      = process_stock(crudos['stock'])
    df_ventas     = process_ventas(crudos['ventas'], df_stock_crudo)
    df_flujo_caja = process_flujo_caja(crudos['flujo_caja'])

    # Guardar cache TC en disco: precargar_tc_fechas ya lo hace para las
    # fechas de Ventas, pero process_stock/process_flujo_caja tambien agregan
    # fechas nuevas a traves de get_tipo_cambio() (usado por convertir_monedas)
    # sin persistirlas -- sin este guardado, esas fechas (Stock puede tener
    # historial de varios anios) se re-consultarian desde cero en cada corrida.
    _guardar_cache_disco()

    # Subir a SharePoint
    upload_to_sharepoint({
        "VENTAS":     df_ventas,
        "STOCK":      df_stock,
        "FLUJO_CAJA": df_flujo_caja,
    })

    print("\n" + "="*70)
    print("   PIPELINE COMPLETADO")
    print("="*70)


if __name__ == "__main__":
    main()
