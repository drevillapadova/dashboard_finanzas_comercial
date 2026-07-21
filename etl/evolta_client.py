"""Cliente directo a EVOLTA para los reportes Ventas/Stock/Flujo de Caja.

Reemplaza la extraccion por Selenium (abrir navegador headless, loguearse
como una persona, hacer clic en "Exportar" y esperar la descarga) por
peticiones HTTP directas -- mismo patron ya probado en
EVOLTA/online-deploy/src/evolta_client.py (login con requests.Session).

Los 3 reportes viven en una API separada (apiwebcr.evoltacomunica.com),
distinta del sitio principal (v4.evolta.pe). Esa API exige un
`Authorization: Bearer <token>` que EVOLTA no expone por una llamada de
red -- lo incrusta como variable de JavaScript en el HTML de cualquier
pagina de Reportes (`var TokenApi = '...'`, junto a IdUsuario/IdEmpresa).
Mapeado 2026-07-20 inspeccionando las peticiones reales del navegador.
"""
import re
from io import BytesIO

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

LOGIN_URL = "https://v4.evolta.pe/Login/Acceso/Logearse"
TOKEN_SOURCE_PAGE = "https://v4.evolta.pe/Reportes/RepVenta/Index"
API_REPORTES_BASE = "https://apiwebcr.evoltacomunica.com:9011"

_TOKEN_RE = re.compile(r"var\s+TokenApi\s*=\s*'([^']+)'")
_ID_USUARIO_RE = re.compile(r"var\s+IdUsuario\s*=\s*(\d+)")
_ID_EMPRESA_RE = re.compile(r"var\s+IdEmpresa\s*=\s*(\d+)")


class EvoltaDirectClient:
    """Login + descarga de los 3 reportes del dashboard, sin navegador."""

    def __init__(self, usuario: str, clave: str):
        if not usuario or not clave:
            raise RuntimeError("Faltan EVOLTA_USER/EVOLTA_PASS")
        self.usuario = usuario
        self.clave = clave
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "Origin": "https://v4.evolta.pe",
            "Referer": TOKEN_SOURCE_PAGE,
        })
        self._id_usuario = None
        self._id_empresa = None
        self._token = None

    def login(self):
        r = self.session.post(
            LOGIN_URL,
            json={"usuario": self.usuario, "clave": self.clave, "ipInfo": '{"usuario":"hola"}'},
            timeout=30,
        )
        r.raise_for_status()
        try:
            redirect = r.json()
            if isinstance(redirect, str) and redirect.startswith("/"):
                self.session.get(f"https://v4.evolta.pe{redirect}", timeout=30)
        except ValueError:
            pass

    def _cargar_token_api(self):
        """Saca TokenApi/IdUsuario/IdEmpresa del HTML de una pagina de Reportes.
        El mismo token sirve para los 3 reportes dentro de la misma sesion
        (confirmado 2026-07-20)."""
        html = self.session.get(TOKEN_SOURCE_PAGE, timeout=30).text
        m_token = _TOKEN_RE.search(html)
        m_user = _ID_USUARIO_RE.search(html)
        m_emp = _ID_EMPRESA_RE.search(html)
        if not (m_token and m_user and m_emp):
            raise RuntimeError(
                "No se encontro TokenApi/IdUsuario/IdEmpresa en el HTML de "
                f"{TOKEN_SOURCE_PAGE} -- EVOLTA pudo haber cambiado la pagina."
            )
        self._token = m_token.group(1)
        self._id_usuario = int(m_user.group(1))
        self._id_empresa = int(m_emp.group(1))

    def _api_headers(self):
        if not self._token:
            self._cargar_token_api()
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    def calentar_conexion_reportes(self):
        """Fuerza el handshake TCP/TLS contra apiwebcr.evoltacomunica.com antes
        del primer reporte real. En pruebas (2026-07-20), la primera peticion
        de la sesion contra ese host fallaba de forma intermitente (400 /
        timeout / conexion reiniciada) sin importar cual reporte fuera --
        una vez abierta la conexion (aunque esta primera respuesta sea un
        error), las siguientes peticiones por la misma conexion funcionaron
        siempre. El resultado de esta llamada se descarta a proposito."""
        try:
            self.session.get(f"{API_REPORTES_BASE}/", headers=self._api_headers(), timeout=15)
        except requests.RequestException:
            pass

    @retry(
        # apiwebcr.evoltacomunica.com es un host separado del sitio principal;
        # la primera peticion de la sesion contra ese host falla de forma
        # intermitente (400, timeout o conexion reiniciada), sin importar cual
        # de los 3 reportes sea. Backend interno no documentado -- se acepta
        # el riesgo (mismo criterio que FASE2_EVOLTA_NOTES.md) y se cubre con
        # reintentos generosos en vez de intentar eliminarlo del todo.
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError, requests.HTTPError)),
        wait=wait_fixed(8),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _post_reporte(self, endpoint: str, payload: dict, timeout: int = 120) -> bytes:
        r = self.session.post(
            f"{API_REPORTES_BASE}/api/v1/reportes/{endpoint}",
            json=payload,
            headers=self._api_headers(),
            timeout=timeout,
        )
        r.raise_for_status()
        return r.content

    def export_ventas(self, fecha_inicio: str, fecha_fin: str) -> bytes:
        """fecha_inicio/fecha_fin en formato DD/MM/YYYY."""
        return self._post_reporte("ventas", {
            "idUsuario": self._id_usuario, "idEmpresa": self._id_empresa,
            "idProyecto": "0", "idEtapa": "0", "idTipoInmueble": "0", "idVendedor": "0",
            "estadoVenta": "0", "fechaInicio": fecha_inicio, "fechaFin": fecha_fin,
            "EsMultimoneda": 1, "nombres": "", "numDocumento": "", "tipoArchivo": "1",
        }, timeout=180)

    def export_stock(self) -> bytes:
        # Evolta tarda en generar este reporte (el Selenium original ya
        # esperaba hasta 480s por la descarga) -- timeout generoso a proposito.
        return self._post_reporte("stock-comercial", {
            "idUsuario": self._id_usuario, "idEmpresa": self._id_empresa,
            "idProyecto": "0", "idEtapa": "0", "idEdificio": 0, "idTipoInmueble": "0",
            "nroInmueble": "", "EsMultimoneda": 1, "tipoArchivo": "1",
        }, timeout=480)

    def export_flujo_caja(self, anio_inicio: str, mes_inicio: str, anio_fin: str, mes_fin: str) -> bytes:
        # Mismo motivo que export_stock -- Selenium original esperaba 480s aqui tambien.
        return self._post_reporte("flujo-caja", {
            "idUsuario": self._id_usuario, "idEmpresa": self._id_empresa,
            "idProyecto": 0, "idEtapa": None, "idEtapaComercial": "0",
            "anioInicio": anio_inicio, "mesInicio": mes_inicio,
            "anioFin": anio_fin, "mesFin": mes_fin,
            "EsMultimoneda": 1, "tipoArchivo": "1",
        }, timeout=480)


def leer_excel_bytes(contenido: bytes):
    """Envuelve pd.read_excel para no importar pandas en cada caller."""
    import pandas as pd
    return pd.read_excel(BytesIO(contenido))


__all__ = ["EvoltaDirectClient", "leer_excel_bytes"]
