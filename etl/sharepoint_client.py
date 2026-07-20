"""Cliente Microsoft Graph minimo para subir/descargar el Excel del dashboard.

Mismo patron que tc-service/src/sharepoint_client.py (client credentials con
msal + requests puro contra Graph v1.0), copiado y recortado para no depender
de otro repo. Requiere permiso de aplicacion `Sites.ReadWrite.All` en el App
Registration (el mismo que ya usan online-deploy y tc-service).
"""
import os
from pathlib import Path

import msal
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]


class SharePointClient:
    """Wrapper minimo sobre Microsoft Graph: solo descarga y sube archivos."""

    def __init__(self):
        self.tenant_id = os.environ["AZURE_TENANT_ID"]
        self.client_id = os.environ["AZURE_CLIENT_ID"]
        self.client_secret = os.environ["AZURE_CLIENT_SECRET"]
        self.hostname = os.environ["SHAREPOINT_HOSTNAME"]
        self.site_path = os.environ["SHAREPOINT_SITE_PATH"]
        self.drive_name = os.environ.get("SHAREPOINT_DRIVE_NAME", "Documents")

        self._site_id = None
        self._drive_id = None
        self._app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

    # ---------------- Auth ----------------
    def _get_token(self):
        result = self._app.acquire_token_silent(SCOPES, account=None)
        if not result:
            result = self._app.acquire_token_for_client(scopes=SCOPES)
        if not result or "access_token" not in result:
            err = result.get("error_description") if result else "sin respuesta"
            raise RuntimeError(f"Fallo autenticacion Azure AD: {err}")
        return result["access_token"]

    def _headers(self, extra=None):
        headers = {"Authorization": f"Bearer {self._get_token()}"}
        if extra:
            headers.update(extra)
        return headers

    # ---------------- HTTP wrappers ----------------
    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _get(self, url, stream=False):
        r = requests.get(url, headers=self._headers(), stream=stream, timeout=120)
        if r.status_code >= 400:
            raise requests.HTTPError(f"GET {url} -> {r.status_code} {r.text[:300]}", response=r)
        return r

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _put(self, url, data, content_type="application/octet-stream"):
        r = requests.put(url, headers=self._headers({"Content-Type": content_type}), data=data, timeout=300)
        if r.status_code >= 400:
            raise requests.HTTPError(f"PUT {url} -> {r.status_code} {r.text[:300]}", response=r)
        return r

    # ---------------- Site / drive ----------------
    @property
    def site_id(self):
        if not self._site_id:
            site_url = f"{self.hostname}:/{self.site_path}"
            self._site_id = self._get(f"{GRAPH_BASE}/sites/{site_url}").json()["id"]
        return self._site_id

    @property
    def drive_id(self):
        if not self._drive_id:
            r = self._get(f"{GRAPH_BASE}/sites/{self.site_id}/drives").json()
            wanted = self.drive_name.lower()
            for d in r.get("value", []):
                if d.get("name", "").lower() == wanted:
                    self._drive_id = d["id"]
                    break
            if not self._drive_id and r.get("value"):
                self._drive_id = r["value"][0]["id"]
        if not self._drive_id:
            raise RuntimeError("No se pudo resolver el drive de SharePoint")
        return self._drive_id

    # ---------------- Download / upload ----------------
    def download_to(self, remote_path, dest: Path):
        """Descarga un archivo (por path relativo al drive) a `dest`."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{remote_path.strip('/')}:/content"
        with self._get(url, stream=True) as r:
            with dest.open("wb") as fh:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        return dest

    def upload_file(self, local_path: Path, remote_folder, remote_name=None):
        """Sube un archivo a una carpeta de SharePoint (overwrite)."""
        remote_name = remote_name or local_path.name
        remote_folder = remote_folder.strip("/")
        target = f"{remote_folder}/{remote_name}"
        size = local_path.stat().st_size

        if size <= 4 * 1024 * 1024:  # < 4MB: simple upload
            url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{target}:/content"
            self._put(url, local_path.read_bytes())
            return

        # Upload session para archivos grandes
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{target}:/createUploadSession"
        session = requests.post(
            url,
            headers=self._headers({"Content-Type": "application/json"}),
            json={"item": {"@microsoft.graph.conflictBehavior": "replace", "name": remote_name}},
            timeout=60,
        ).json()
        upload_url = session.get("uploadUrl")
        if not upload_url:
            raise RuntimeError(f"No upload URL: {session}")

        chunk_size = 5 * 1024 * 1024
        with local_path.open("rb") as fh:
            offset = 0
            while offset < size:
                chunk = fh.read(chunk_size)
                end = offset + len(chunk) - 1
                resp = requests.put(
                    upload_url,
                    headers={
                        "Content-Length": str(len(chunk)),
                        "Content-Range": f"bytes {offset}-{end}/{size}",
                    },
                    data=chunk,
                    timeout=300,
                )
                if resp.status_code >= 400:
                    raise RuntimeError(f"Upload chunk fallo: {resp.status_code} {resp.text[:300]}")
                offset = end + 1


__all__ = ["SharePointClient"]
