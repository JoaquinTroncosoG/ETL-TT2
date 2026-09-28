#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extraccion de YouTube (Data API v3)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Extrae, en tres pasos, los datos de un canal de YouTube:
    1. Informacion del canal          GET /channels
    2. Todos los videos del canal     GET /playlistItems + /videos  (paginacion por token)
    3. Metricas de cada video         GET /videos (statistics + contentDetails)

Guarda las respuestas crudas de la API, sin transformarlas, en un unico JSON
junto con los metadatos de la ejecucion (alimentan la dimension de auditoria
del Data Warehouse). La etapa de extraccion no transforma: solo observa y
fecha lo observado.

USO
---
    python src/extract/extract_youtube.py
    python src/extract/extract_youtube.py --limite 10            # prueba corta
    python src/extract/extract_youtube.py --sin-estadisticas     # solo canal y listado
    python src/extract/extract_youtube.py --salida data/raw/pruebas

Variables requeridas en el .env: YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID

Codigos de salida: 0 exito, 1 fallida, 130 interrumpida por el usuario.
"""

import argparse
import datetime as dt
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Falta la libreria 'requests'. Instala con: pip install -r requirements.txt")

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("Falta la libreria 'python-dotenv'. Instala con: pip install -r requirements.txt")

# --------------------------------------------------------------------------
# Configuracion
# --------------------------------------------------------------------------

PLATAFORMA = "youtube"
VERSION_API = "v3"
BASE_URL = "https://www.googleapis.com/youtube/v3"
TIMEOUT = 30

RAIZ = Path(__file__).resolve().parents[2]
SALIDA_POR_DEFECTO = RAIZ / "data" / "raw" / "youtube"

CAMPOS_CANAL = "snippet,statistics,contentDetails,brandingSettings"
CAMPOS_PLAYLIST = "snippet,contentDetails,status"
CAMPOS_VIDEO = "snippet,statistics,contentDetails,topicDetails"
TAMANO_PAGINA = 50

# Codigos de error de la YouTube Data API
CODIGO_CUOTA_AGOTADA = 403
CODIGO_NO_ENCONTRADO = 404
CODIGO_NO_AUTORIZADO = 401

# Reintentos
MAX_REINTENTOS_CUOTA = 3
ESPERA_BASE_CUOTA = 60
ESPERA_MAXIMA_CUOTA = 600
MAX_REINTENTOS_RED = 5
ESPERA_BASE_RED = 5

MENSAJE_API_KEY = (
    "La API Key de YouTube es invalida o no tiene permisos (codigo 401/403). "
    "Verifica que YOUTUBE_API_KEY en el .env sea correcta y que la YouTube "
    "Data API v3 este habilitada en tu proyecto de Google Cloud Console."
)

log = logging.getLogger("extract_youtube")

# --------------------------------------------------------------------------
# Proteccion de la API Key: nunca debe aparecer en pantalla, logs ni archivos
# --------------------------------------------------------------------------

_SECRETOS = []
_PATRON_KEY = re.compile(r"(key=)[^\&\s'\"]+", re.IGNORECASE)


def ocultar_secretos(texto):
    """Reemplaza la API Key (literal o como parametro de URL) por ***."""
    texto = str(texto)
    for secreto in _SECRETOS:
        texto = texto.replace(secreto, "***")
    return _PATRON_KEY.sub(r"\1***", texto)


class FiltroSecretos(logging.Filter):
    """Filtro de logging que enmascara la API Key en todos los mensajes."""

    def filter(self, record):
        record.msg = ocultar_secretos(record.getMessage())
        record.args = ()
        return True


def configurar_logging():
    manejador = logging.StreamHandler(sys.stdout)
    manejador.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                                             datefmt="%H:%M:%S"))
    manejador.addFilter(FiltroSecretos())
    logging.basicConfig(level=logging.INFO, handlers=[manejador])


def ahora_utc():
    return dt.datetime.now(dt.timezone.utc)


def iso(momento):
    return momento.isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Errores
# --------------------------------------------------------------------------

class ErrorExtraccion(Exception):
    """Error que detiene la extraccion."""

    def como_dict(self):
        return {"tipo": type(self).__name__, "mensaje": ocultar_secretos(self)}


class ApiKeyInvalida(ErrorExtraccion):
    pass


class CuotaAgotada(ErrorExtraccion):
    pass


class ErrorApi(ErrorExtraccion):
    """Error HTTP devuelto por la YouTube Data API."""

    def __init__(self, mensaje, estado_http=None, error_youtube=None):
        super().__init__(ocultar_secretos(mensaje))
        self.mensaje = ocultar_secretos(mensaje)
        self.estado_http = estado_http
        error_youtube = error_youtube or {}
        self.codigo = error_youtube.get("code")
        self.razon = error_youtube.get("reason")

    def como_dict(self):
        return {
            "tipo": type(self).__name__,
            "mensaje": self.mensaje,
            "estado_http": self.estado_http,
            "codigo": self.codigo,
            "razon": self.razon,
        }


# --------------------------------------------------------------------------
# Cliente de la YouTube Data API con reintentos
# --------------------------------------------------------------------------

class ClienteYouTube:

    def __init__(self, api_key):
        self._api_key = api_key
        self._sesion = requests.Session()
        self.llamadas = 0
        self.cuota_estimada = 0  # Cada llamada a list cuesta ~1 unidad de cuota

    def get(self, recurso, params=None):
        """
        Ejecuta un GET al endpoint indicado.
        `recurso` es el nombre del recurso ('channels', 'playlistItems', 'videos').
        """
        url = f"{BASE_URL}/{recurso}"
        consulta = dict(params or {})
        consulta["key"] = self._api_key

        intentos_cuota = 0
        intentos_red = 0
        while True:
            self.llamadas += 1
            self.cuota_estimada += 1
            try:
                resp = self._sesion.get(url, params=consulta, timeout=TIMEOUT)
            except requests.RequestException as exc:
                intentos_red += 1
                if intentos_red > MAX_REINTENTOS_RED:
                    raise ErrorExtraccion(
                        f"Error de red persistente tras {MAX_REINTENTOS_RED} reintentos: "
                        f"{ocultar_secretos(exc)}") from None
                self._esperar(self._backoff(ESPERA_BASE_RED, intentos_red, 600),
                              f"Error de red ({type(exc).__name__})",
                              intentos_red, MAX_REINTENTOS_RED)
                continue

            cuerpo = self._leer_json(resp)

            if resp.ok:
                return cuerpo

            # Extraer detalles del error de YouTube
            error_info = {}
            if isinstance(cuerpo, dict) and "error" in cuerpo:
                error_obj = cuerpo["error"]
                errores = error_obj.get("errors", [{}])
                if errores:
                    error_info = errores[0]
                mensaje = error_obj.get("message", f"HTTP {resp.status_code}")
                codigo = error_obj.get("code", resp.status_code)
            else:
                mensaje = resp.text[:300] or f"HTTP {resp.status_code}"
                codigo = resp.status_code

            # Token/Key invalida
            if codigo == CODIGO_NO_AUTORIZADO:
                raise ApiKeyInvalida(MENSAJE_API_KEY)

            # Cuota agotada (YouTube devuelve 403 con reason "quotaExceeded")
            razon = error_info.get("reason", "")
            if codigo == CODIGO_CUOTA_AGOTADA and "quota" in razon.lower():
                intentos_cuota += 1
                if intentos_cuota > MAX_REINTENTOS_CUOTA:
                    raise CuotaAgotada(
                        f"Cuota de la API agotada tras {MAX_REINTENTOS_CUOTA} reintentos "
                        f"(HTTP {codigo}): {ocultar_secretos(mensaje)}")
                self._esperar(self._backoff(ESPERA_BASE_CUOTA, intentos_cuota,
                                            ESPERA_MAXIMA_CUOTA),
                              f"Cuota agotada (HTTP {codigo})",
                              intentos_cuota, MAX_REINTENTOS_CUOTA)
                continue

            # Error del servidor
            if resp.status_code >= 500:
                intentos_red += 1
                if intentos_red > MAX_REINTENTOS_RED:
                    raise ErrorApi(f"Error del servidor de Google tras {MAX_REINTENTOS_RED} "
                                   f"reintentos: {mensaje}", resp.status_code, error_info)
                self._esperar(self._backoff(ESPERA_BASE_RED, intentos_red, 600),
                              f"Error del servidor (HTTP {resp.status_code})",
                              intentos_red, MAX_REINTENTOS_RED)
                continue

            raise ErrorApi(mensaje, resp.status_code, error_info)

    @staticmethod
    def _leer_json(resp):
        try:
            return resp.json()
        except ValueError:
            return {}

    @staticmethod
    def _backoff(base, intento, maximo):
        espera = min(base * 2 ** (intento - 1), maximo)
        return espera + random.uniform(0, espera * 0.1)

    @staticmethod
    def _esperar(segundos, motivo, intento, maximo):
        log.warning("%s. Reintento %d/%d en %.0f s...", motivo, intento, maximo, segundos)
        time.sleep(segundos)


# --------------------------------------------------------------------------
# Paso 1: Informacion del canal
# --------------------------------------------------------------------------

def extraer_canal(cliente, channel_id):
    """Obtiene la informacion completa del canal."""
    respuesta = cliente.get("channels", {
        "part": CAMPOS_CANAL,
        "id": channel_id,
    })
    items = respuesta.get("items", [])
    if not items:
        raise ErrorExtraccion(
            f"No se encontro ningun canal con el ID '{channel_id}'. "
            f"Verifica YOUTUBE_CHANNEL_ID en el .env.")
    return respuesta


# --------------------------------------------------------------------------
# Paso 2: Obtener todos los videos del canal (via uploads playlist)
# --------------------------------------------------------------------------

def extraer_videos_listado(cliente, uploads_playlist_id, limite):
    """
    Recorre la playlist de 'uploads' del canal para obtener todos los video IDs.
    Usa paginacion por nextPageToken.
    Devuelve (lista_de_video_ids, paginas_crudas).
    """
    video_ids = []
    paginas = []
    page_token = None

    while True:
        tamano = TAMANO_PAGINA
        if limite is not None:
            tamano = min(TAMANO_PAGINA, limite - len(video_ids))
            if tamano <= 0:
                break

        params = {
            "part": CAMPOS_PLAYLIST,
            "playlistId": uploads_playlist_id,
            "maxResults": tamano,
        }
        if page_token:
            params["pageToken"] = page_token

        pagina = cliente.get("playlistItems", params)
        paginas.append(pagina)

        items = pagina.get("items", [])
        for item in items:
            vid = item.get("contentDetails", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        log.info("  Pagina %d: %d videos (acumulado %d)",
                 len(paginas), len(items), len(video_ids))

        page_token = pagina.get("nextPageToken")
        if not page_token or not items:
            break

    if limite is not None:
        video_ids = video_ids[:limite]

    return video_ids, paginas


# --------------------------------------------------------------------------
# Paso 3: Estadisticas detalladas de cada video (en lotes de 50)
# --------------------------------------------------------------------------

def extraer_estadisticas_videos(cliente, video_ids):
    """
    Obtiene snippet, statistics, contentDetails y topicDetails de cada video.
    La API permite hasta 50 IDs por llamada, asi que se procesan en lotes.
    Devuelve (lista_de_videos_con_stats, paginas_crudas).
    """
    videos = []
    paginas = []
    total = len(video_ids)

    for i in range(0, total, TAMANO_PAGINA):
        lote = video_ids[i:i + TAMANO_PAGINA]
        respuesta = cliente.get("videos", {
            "part": CAMPOS_VIDEO,
            "id": ",".join(lote),
        })
        paginas.append(respuesta)
        items = respuesta.get("items", [])
        videos.extend(items)

        procesados = min(i + TAMANO_PAGINA, total)
        log.info("  Estadisticas: %d/%d videos (%d llamadas a la API)",
                 procesados, total, cliente.llamadas)

    return videos, paginas


# --------------------------------------------------------------------------
# Credenciales, guardado y programa principal
# --------------------------------------------------------------------------

def cargar_credenciales():
    load_dotenv(RAIZ / ".env")
    api_key = (os.getenv("YOUTUBE_API_KEY") or "").strip()
    channel_id = (os.getenv("YOUTUBE_CHANNEL_ID") or "").strip()

    faltantes = [nombre for nombre, valor in (("YOUTUBE_API_KEY", api_key),
                                              ("YOUTUBE_CHANNEL_ID", channel_id)) if not valor]
    if faltantes:
        log.error("Faltan variables en el .env: %s", ", ".join(faltantes))
        log.error("Copia .env.example a .env y completalas.")
        sys.exit(1)
    return api_key, channel_id


def guardar(resultado, carpeta, inicio, id_ejecucion):
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{PLATAFORMA}_{inicio:%Y%m%d_%H%M%S}_UTC_{id_ejecucion[:8]}.json"
    # Salvaguarda final: la API Key nunca debe quedar escrita en disco
    texto = json.dumps(resultado, ensure_ascii=False, indent=2)
    for secreto in _SECRETOS:
        texto = texto.replace(secreto, "***")
    temporal = ruta.with_suffix(".json.tmp")
    temporal.write_text(texto, encoding="utf-8")
    os.replace(temporal, ruta)
    return ruta


def parsear_argumentos(argv=None):
    parser = argparse.ArgumentParser(
        description="Extrae canal, videos y estadisticas de YouTube (Data API v3) "
                    "y guarda el JSON crudo con metadatos de ejecucion.")
    parser.add_argument("--limite", type=int, metavar="N",
                        help="maximo de videos a extraer (para pruebas)")
    parser.add_argument("--sin-estadisticas", action="store_true",
                        help="omitir estadisticas detalladas; solo canal y listado de videos")
    parser.add_argument("--salida", default=str(SALIDA_POR_DEFECTO), metavar="RUTA",
                        help=f"carpeta de destino (por defecto {SALIDA_POR_DEFECTO})")
    args = parser.parse_args(argv)
    if args.limite is not None and args.limite <= 0:
        parser.error("--limite debe ser un entero mayor que 0")
    return args


def main(argv=None):
    args = parsear_argumentos(argv)
    configurar_logging()
    api_key, channel_id = cargar_credenciales()
    _SECRETOS.append(api_key)

    inicio = ahora_utc()
    reloj = time.monotonic()
    id_ejecucion = str(uuid.uuid4())
    cliente = ClienteYouTube(api_key)

    metadatos = {
        "id_ejecucion": id_ejecucion,
        "plataforma": PLATAFORMA,
        "version_api": VERSION_API,
        "fecha_hora_inicio_utc": iso(inicio),
        "fecha_hora_fin_utc": None,
        "duracion_segundos": None,
        "estado": None,
        "videos_extraidos": 0,
        "llamadas_api": 0,
        "paginas_recorridas": 0,
        "cuota_estimada": 0,
        "error": None,
        "youtube_channel_id": channel_id,
        "parametros": {"limite": args.limite, "sin_estadisticas": args.sin_estadisticas},
    }
    respuestas = {
        "canal": None,
        "paginas_playlist": [],
        "estadisticas_videos": None if args.sin_estadisticas else [],
    }
    video_ids = []

    log.info("Extraccion de YouTube %s | ejecucion %s", VERSION_API, id_ejecucion)
    try:
        # -- Paso 1: Canal --
        log.info("[1/3] Informacion del canal %s", channel_id)
        respuestas["canal"] = extraer_canal(cliente, channel_id)
        canal_info = respuestas["canal"]["items"][0]
        snippet = canal_info.get("snippet", {})
        stats = canal_info.get("statistics", {})
        uploads_id = canal_info.get("contentDetails", {}).get(
            "relatedPlaylists", {}).get("uploads")

        log.info("  Canal: %s", snippet.get("title", "N/A"))
        log.info("  Suscriptores: %s | Videos: %s | Vistas totales: %s",
                 stats.get("subscriberCount", "N/A"),
                 stats.get("videoCount", "N/A"),
                 stats.get("viewCount", "N/A"))

        if not uploads_id:
            raise ErrorExtraccion(
                "No se pudo obtener la playlist de uploads del canal. "
                "El canal podria no tener videos publicos.")

        # -- Paso 2: Listado de videos --
        log.info("[2/3] Videos del canal (playlist: %s)", uploads_id)
        video_ids, respuestas["paginas_playlist"] = extraer_videos_listado(
            cliente, uploads_id, args.limite)

        # -- Paso 3: Estadisticas --
        if args.sin_estadisticas:
            log.info("[3/3] Estadisticas omitidas (--sin-estadisticas)")
        elif video_ids:
            log.info("[3/3] Estadisticas de %d videos", len(video_ids))
            videos_stats, paginas_stats = extraer_estadisticas_videos(cliente, video_ids)
            respuestas["estadisticas_videos"] = paginas_stats
        else:
            log.info("[3/3] Sin videos para consultar estadisticas")

        metadatos["estado"] = "EXITO"
    except ApiKeyInvalida as exc:
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = exc.como_dict()
        log.error(MENSAJE_API_KEY)
    except ErrorExtraccion as exc:
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = exc.como_dict()
        log.error("Extraccion fallida: %s", exc)
    except KeyboardInterrupt:
        metadatos["estado"] = "INTERRUMPIDA"
        metadatos["error"] = {"tipo": "KeyboardInterrupt",
                              "mensaje": "Ejecucion interrumpida por el usuario."}
        log.warning("Ejecucion interrumpida por el usuario; se guardan los datos parciales.")
    except Exception as exc:
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = {"tipo": type(exc).__name__, "mensaje": ocultar_secretos(exc)}
        log.error("Error inesperado: %s: %s", type(exc).__name__, exc)

    fin = ahora_utc()
    metadatos.update({
        "fecha_hora_fin_utc": iso(fin),
        "duracion_segundos": round(time.monotonic() - reloj, 3),
        "videos_extraidos": len(video_ids),
        "llamadas_api": cliente.llamadas,
        "paginas_recorridas": len(respuestas["paginas_playlist"]),
        "cuota_estimada": cliente.cuota_estimada,
    })

    try:
        ruta = guardar({"metadatos_ejecucion": metadatos, "respuestas": respuestas},
                       args.salida, inicio, id_ejecucion)
    except OSError as exc:
        log.error("No se pudo guardar el archivo de salida: %s", exc)
        return 1

    log.info("Estado: %s | %d videos | %d paginas | %d llamadas | %.1f s",
             metadatos["estado"], metadatos["videos_extraidos"],
             metadatos["paginas_recorridas"], metadatos["llamadas_api"],
             metadatos["duracion_segundos"])
    log.info("Archivo guardado en %s", ruta)

    return {"EXITO": 0, "INTERRUMPIDA": 130}.get(metadatos["estado"], 1)


if __name__ == "__main__":
    sys.exit(main())
