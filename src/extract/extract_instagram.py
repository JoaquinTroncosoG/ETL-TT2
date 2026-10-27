#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extraccion de Instagram (Graph API)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Extrae, en tres pasos, los datos de una cuenta de Instagram Business:
    1. Perfil de la cuenta          GET /{ig-user-id}
    2. Todas las publicaciones      GET /{ig-user-id}/media   (paginacion por cursor)
    3. Metricas de cada publicacion GET /{media-id}/insights

Guarda las respuestas crudas de la API, sin transformarlas, en un unico JSON
junto con los metadatos de la ejecucion (alimentan la dimension de auditoria
del Data Warehouse). La etapa de extraccion no transforma: solo observa y
fecha lo observado.

USO
---
    python src/extract/extract_instagram.py
    python src/extract/extract_instagram.py --limite 5            # prueba corta
    python src/extract/extract_instagram.py --sin-insights        # solo perfil y publicaciones
    python src/extract/extract_instagram.py --tamano-pagina 2      # forzar varias paginas
    python src/extract/extract_instagram.py --salida data/raw/pruebas

Variables requeridas en el .env: INSTAGRAM_ACCESS_TOKEN, IG_BUSINESS_ACCOUNT_ID

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
from urllib.parse import parse_qsl, urlsplit, urlunsplit

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

PLATAFORMA = "instagram"
VERSION_API = "v21.0"
GRAPH = f"https://graph.facebook.com/{VERSION_API}"
TIMEOUT = 30

RAIZ = Path(__file__).resolve().parents[2]
SALIDA_POR_DEFECTO = RAIZ / "data" / "raw" / "instagram"

CAMPOS_PERFIL = "id,username,name,followers_count,follows_count,media_count"
CAMPOS_MEDIA = ("id,caption,media_type,media_product_type,permalink,"
                "timestamp,like_count,comments_count")
TAMANO_PAGINA = 50

METRICAS_BASE = ["reach", "saved"]
METRICAS_IMAGEN_VIDEO = ["likes", "comments", "shares"]
METRICAS_REELS = ["ig_reels_aggregated_all_plays_count"]

# Codigos de Meta que indican cuota agotada. 80002 es el limite propio de
# la cuenta de Instagram (Business Use Case), equivalente a los anteriores.
CODIGOS_CUOTA = {4, 17, 32, 613, 80002}
CODIGO_TOKEN_INVALIDO = 190
CODIGO_PARAMETRO_INVALIDO = 100

# El limite es ~200 llamadas por hora: las esperas por cuota son largas.
MAX_REINTENTOS_CUOTA = 6
ESPERA_BASE_CUOTA = 60          # segundos: 60, 120, 240, 480, 960, 1920
ESPERA_MAXIMA_CUOTA = 3600
MAX_REINTENTOS_RED = 5
ESPERA_BASE_RED = 5             # segundos: 5, 10, 20, 40, 80
UMBRAL_AVISO_CUOTA = 80         # % de uso a partir del cual se avisa

MENSAJE_TOKEN = (
    "El token de acceso de Instagram es invalido o esta vencido (codigo 190). "
    "Los tokens de larga duracion de Meta duran 60 dias y luego hay que "
    "regenerarlos: obten un token corto en el Explorador de la Graph API, "
    "canjealo con 'python verificar_credenciales.py --canjear-token <TOKEN_CORTO>' "
    "y actualiza INSTAGRAM_ACCESS_TOKEN e INSTAGRAM_TOKEN_EXPIRA en el .env."
)

log = logging.getLogger("extract_instagram")

# --------------------------------------------------------------------------
# Proteccion del token: nunca debe aparecer en pantalla, logs ni archivos
# --------------------------------------------------------------------------

_SECRETOS = []
_PATRON_TOKEN = re.compile(r"(access_token=)[^&\s'\"]+", re.IGNORECASE)


def ocultar_secretos(texto):
    """Reemplaza el token (literal o como parametro de URL) por ***."""
    texto = str(texto)
    for secreto in _SECRETOS:
        texto = texto.replace(secreto, "***")
    return _PATRON_TOKEN.sub(r"\1***", texto)


class FiltroSecretos(logging.Filter):
    """Filtro de logging que enmascara el token en todos los mensajes."""

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


class TokenInvalido(ErrorExtraccion):
    pass


class CuotaAgotada(ErrorExtraccion):
    pass


class ErrorApi(ErrorExtraccion):
    """Error 4xx devuelto por la Graph API, con el detalle de Meta."""

    def __init__(self, mensaje, estado_http=None, error_meta=None):
        super().__init__(ocultar_secretos(mensaje))
        self.mensaje = ocultar_secretos(mensaje)
        self.estado_http = estado_http
        error_meta = error_meta or {}
        self.codigo = error_meta.get("code")
        self.subcodigo = error_meta.get("error_subcode")
        self.fbtrace_id = error_meta.get("fbtrace_id")

    def como_dict(self):
        return {
            "tipo": type(self).__name__,
            "mensaje": self.mensaje,
            "estado_http": self.estado_http,
            "codigo": self.codigo,
            "subcodigo": self.subcodigo,
            "fbtrace_id": self.fbtrace_id,
        }


# --------------------------------------------------------------------------
# Cliente de la Graph API con reintentos
# --------------------------------------------------------------------------

class ClienteGraph:

    def __init__(self, token):
        self._token = token
        self._sesion = requests.Session()
        self._minutos_para_recuperar = 0
        self._ultimo_aviso_cuota = 0
        self.llamadas = 0
        self.historial_uso = []     # valores de X-Business-Use-Case-Usage

    def get(self, destino, params=None):
        """
        Ejecuta un GET y devuelve el JSON crudo.
        `destino` es una ruta relativa ('123/media') o una URL absoluta
        (paging.next). El token se quita de la URL y se envia aparte.
        """
        if destino.startswith("http"):
            partes = urlsplit(destino)
            consulta = {k: v for k, v in parse_qsl(partes.query) if k != "access_token"}
            url = urlunsplit((partes.scheme, partes.netloc, partes.path, "", ""))
        else:
            consulta = {}
            url = f"{GRAPH}/{destino.lstrip('/')}"
        consulta.update(params or {})
        consulta["access_token"] = self._token

        intentos_cuota = 0
        intentos_red = 0
        while True:
            self.llamadas += 1
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

            self._registrar_uso(resp)
            cuerpo = self._leer_json(resp)
            error = cuerpo.get("error") if isinstance(cuerpo, dict) else None
            if resp.ok and not error:
                return cuerpo

            error = error if isinstance(error, dict) else {}
            codigo = error.get("code")
            mensaje = error.get("message") or resp.text[:300] or f"HTTP {resp.status_code}"

            if codigo == CODIGO_TOKEN_INVALIDO:
                raise TokenInvalido(MENSAJE_TOKEN)

            if resp.status_code == 429 or codigo in CODIGOS_CUOTA:
                intentos_cuota += 1
                if intentos_cuota > MAX_REINTENTOS_CUOTA:
                    raise CuotaAgotada(
                        f"Cuota de la API agotada tras {MAX_REINTENTOS_CUOTA} reintentos "
                        f"(codigo {codigo}, HTTP {resp.status_code}): {ocultar_secretos(mensaje)}")
                self._esperar(self._espera_cuota(intentos_cuota),
                              f"Cuota agotada (codigo {codigo}, HTTP {resp.status_code})",
                              intentos_cuota, MAX_REINTENTOS_CUOTA)
                continue

            if resp.status_code >= 500:
                intentos_red += 1
                if intentos_red > MAX_REINTENTOS_RED:
                    raise ErrorApi(f"Error del servidor de Meta tras {MAX_REINTENTOS_RED} "
                                   f"reintentos: {mensaje}", resp.status_code, error)
                self._esperar(self._backoff(ESPERA_BASE_RED, intentos_red, 600),
                              f"Error del servidor (HTTP {resp.status_code})",
                              intentos_red, MAX_REINTENTOS_RED)
                continue

            raise ErrorApi(mensaje, resp.status_code, error)

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

    def _espera_cuota(self, intento):
        espera = self._backoff(ESPERA_BASE_CUOTA, intento, ESPERA_MAXIMA_CUOTA)
        # Si Meta informa cuanto falta para recuperar la cuota, se respeta.
        if self._minutos_para_recuperar:
            espera = max(espera, min(self._minutos_para_recuperar * 60, ESPERA_MAXIMA_CUOTA))
        return espera

    @staticmethod
    def _esperar(segundos, motivo, intento, maximo):
        log.warning("%s. Reintento %d/%d en %.0f s...", motivo, intento, maximo, segundos)
        time.sleep(segundos)

    def _registrar_uso(self, resp):
        """Guarda la cabecera X-Business-Use-Case-Usage para saber cuanta cuota se uso."""
        cabecera = resp.headers.get("X-Business-Use-Case-Usage")
        if not cabecera:
            return
        try:
            valor = json.loads(cabecera)
        except ValueError:
            valor = cabecera
        self.historial_uso.append({
            "llamada": self.llamadas,
            "fecha_hora_utc": iso(ahora_utc()),
            "valor": valor,
        })

        porcentaje, minutos = 0, 0
        if isinstance(valor, dict):
            for entradas in valor.values():
                for entrada in entradas if isinstance(entradas, list) else []:
                    try:
                        porcentaje = max(porcentaje, *(float(entrada.get(k) or 0) for k in
                                                       ("call_count", "total_cputime", "total_time")))
                        minutos = max(minutos, float(entrada.get("estimated_time_to_regain_access") or 0))
                    except (TypeError, ValueError, AttributeError):
                        continue
        self._minutos_para_recuperar = minutos
        # Se avisa al cruzar el umbral y luego cada 5 puntos, no en cada llamada
        if porcentaje < UMBRAL_AVISO_CUOTA:
            self._ultimo_aviso_cuota = 0
        elif porcentaje >= self._ultimo_aviso_cuota + 5:
            self._ultimo_aviso_cuota = porcentaje
            log.warning("Uso de cuota de la API al %.0f%%.", porcentaje)


# --------------------------------------------------------------------------
# Paso 1 y 2: perfil y publicaciones
# --------------------------------------------------------------------------

def redactar_paginacion(pagina):
    """Quita el token de las URLs de paging.next/previous (Meta lo incluye ahi)."""
    paging = pagina.get("paging") if isinstance(pagina, dict) else None
    if isinstance(paging, dict):
        for clave in ("next", "previous"):
            if isinstance(paging.get(clave), str):
                paging[clave] = ocultar_secretos(paging[clave])
    return pagina


def extraer_publicaciones(cliente, ig_id, limite, paginas, tamano_pagina=TAMANO_PAGINA):
    """
    Recorre todas las paginas siguiendo paging.next. Cada pagina cruda se
    agrega a `paginas` apenas llega, para no perderla si la ejecucion falla.
    Devuelve la lista de publicaciones (para pedir sus insights).

    `tamano_pagina` es cuantas publicaciones se piden por peticion. Bajarlo
    obliga a la API a devolver varias paginas aunque la cuenta tenga pocas
    publicaciones, lo que permite verificar la paginacion contra la API real.
    """
    publicaciones = []
    destino = f"{ig_id}/media"
    while destino:
        tamano = tamano_pagina
        if limite is not None:
            tamano = min(tamano_pagina, limite - len(publicaciones))
            if tamano <= 0:
                break

        pagina = cliente.get(destino, {"fields": CAMPOS_MEDIA, "limit": tamano})
        # paging.next se lee antes de redactar la URL
        destino = (pagina.get("paging") or {}).get("next")
        paginas.append(redactar_paginacion(pagina))

        datos = pagina.get("data") or []
        publicaciones.extend(datos)
        log.info("  Pagina %d: %d publicaciones (acumulado %d)",
                 len(paginas), len(datos), len(publicaciones))
        if not datos:
            break

    return publicaciones if limite is None else publicaciones[:limite]


# --------------------------------------------------------------------------
# Paso 3: insights con degradacion de metricas
# --------------------------------------------------------------------------

def metricas_para(media):
    metricas = list(METRICAS_BASE)
    if media.get("media_type") in ("IMAGE", "VIDEO"):
        metricas += METRICAS_IMAGEN_VIDEO
    if media.get("media_product_type") == "REELS":
        metricas += METRICAS_REELS
    return metricas


def identificar_metricas_rechazadas(exc, metricas):
    """Intenta deducir del mensaje de Meta que metrica(s) rechazo la API."""
    mensaje = exc.mensaje or ""

    # "(#100) metric[2] must be one of the following values: ..."
    indice = re.search(r"metric\[(\d+)\]", mensaje)
    if indice and int(indice.group(1)) < len(metricas):
        return {metricas[int(indice.group(1))]: mensaje}

    # Lista de valores permitidos sin indice: se descarta lo que no este en ella
    permitidas = re.search(r"must be one of the following values:\s*(.+)", mensaje,
                           re.IGNORECASE | re.DOTALL)
    if permitidas:
        validas = {v.strip(" .") for v in permitidas.group(1).split(",")}
        rechazadas = {m: mensaje for m in metricas if m not in validas}
        if 0 < len(rechazadas) < len(metricas):
            return rechazadas

    # "...does not support the video_views metric for this media product type"
    nombradas = {m: mensaje for m in metricas
                 if re.search(rf"(?<!\w){re.escape(m)}(?!\w)", mensaje)}
    if 0 < len(nombradas) < len(metricas):
        return nombradas
    return {}


def sondear_metricas(cliente, media_id, metricas):
    """Ultimo recurso: pide cada metrica por separado para aislar la rechazada."""
    log.info("  No se pudo identificar la metrica rechazada; probando una por una...")
    rechazadas = {}
    for metrica in metricas:
        try:
            cliente.get(f"{media_id}/insights", {"metric": metrica})
        except ErrorApi as exc:
            if exc.codigo != CODIGO_PARAMETRO_INVALIDO:
                raise
            rechazadas[metrica] = exc.mensaje
    # Si fallan todas, el problema es la publicacion y no las metricas
    return rechazadas if len(rechazadas) < len(metricas) else {}


def insights_de_publicacion(cliente, media, descartadas_por_tipo):
    """
    Pide las metricas de una publicacion. Si la API rechaza alguna, la saca
    de la lista y reintenta con las restantes. Las metricas rechazadas se
    recuerdan por tipo de publicacion para no gastar cuota repitiendo el error.
    """
    tipo = f"{media.get('media_product_type')}/{media.get('media_type')}"
    descartadas = descartadas_por_tipo.setdefault(tipo, {})
    candidatas = metricas_para(media)
    metricas = [m for m in candidatas if m not in descartadas]

    def registro(respuesta=None, error=None):
        return {
            "tipo_publicacion": tipo,
            "metricas_solicitadas": metricas,
            "metricas_descartadas": [m for m in candidatas if m in descartadas],
            "respuesta": respuesta,
            "error": error,
        }

    while metricas:
        try:
            respuesta = cliente.get(f"{media['id']}/insights", {"metric": ",".join(metricas)})
            return registro(respuesta=respuesta)
        except ErrorApi as exc:
            # Solo el codigo 100 (parametro invalido) puede deberse a una metrica;
            # cualquier otro error 4xx detiene la extraccion.
            if exc.codigo != CODIGO_PARAMETRO_INVALIDO:
                raise
            rechazadas = identificar_metricas_rechazadas(exc, metricas)
            if not rechazadas and exc.subcodigo is None and len(metricas) > 1:
                rechazadas = sondear_metricas(cliente, media["id"], metricas)
            if not rechazadas:
                # Error propio de la publicacion (p. ej. publicada antes de
                # convertir la cuenta a Business): se registra y se sigue.
                log.warning("  Publicacion %s sin insights: %s", media["id"], exc.mensaje)
                return registro(error=exc.como_dict())
            for metrica, motivo in rechazadas.items():
                descartadas[metrica] = motivo
                log.warning("  La API rechazo la metrica '%s' para %s; se descarta y se "
                            "reintenta con las restantes.", metrica, tipo)
            metricas = [m for m in metricas if m not in rechazadas]

    return registro(error={"tipo": "SinMetricas",
                           "mensaje": "La API no acepto ninguna metrica para este tipo de publicacion."})


def extraer_insights(cliente, publicaciones, insights, descartadas_por_tipo):
    total = len(publicaciones)
    for i, media in enumerate(publicaciones, 1):
        insights[media["id"]] = insights_de_publicacion(cliente, media, descartadas_por_tipo)
        if i % 10 == 0 or i == total:
            log.info("  Insights: %d/%d publicaciones (%d llamadas a la API)",
                     i, total, cliente.llamadas)


# --------------------------------------------------------------------------
# Credenciales, guardado y programa principal
# --------------------------------------------------------------------------

def cargar_credenciales():
    load_dotenv(RAIZ / ".env")
    token = (os.getenv("INSTAGRAM_ACCESS_TOKEN") or "").strip()
    ig_id = (os.getenv("IG_BUSINESS_ACCOUNT_ID") or "").strip()

    faltantes = [nombre for nombre, valor in (("INSTAGRAM_ACCESS_TOKEN", token),
                                              ("IG_BUSINESS_ACCOUNT_ID", ig_id)) if not valor]
    if faltantes:
        log.error("Faltan variables en el .env: %s", ", ".join(faltantes))
        log.error("Copia .env.example a .env y completalas. IG_BUSINESS_ACCOUNT_ID se "
                  "obtiene ejecutando 'python verificar_credenciales.py'.")
        sys.exit(1)
    return token, ig_id


def guardar(resultado, carpeta, inicio, id_ejecucion):
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{PLATAFORMA}_{inicio:%Y%m%d_%H%M%S}_UTC_{id_ejecucion[:8]}.json"
    # Salvaguarda final: el token nunca debe quedar escrito en disco
    texto = json.dumps(resultado, ensure_ascii=False, indent=2)
    for secreto in _SECRETOS:
        texto = texto.replace(secreto, "***")
    temporal = ruta.with_suffix(".json.tmp")
    temporal.write_text(texto, encoding="utf-8")
    os.replace(temporal, ruta)
    return ruta


def parsear_argumentos(argv=None):
    parser = argparse.ArgumentParser(
        description="Extrae perfil, publicaciones e insights de Instagram (Graph API) "
                    "y guarda el JSON crudo con metadatos de ejecucion.")
    parser.add_argument("--limite", type=int, metavar="N",
                        help="maximo de publicaciones a extraer (para pruebas)")
    parser.add_argument("--sin-insights", action="store_true",
                        help="omitir las metricas; solo perfil y publicaciones")
    parser.add_argument("--tamano-pagina", type=int, default=TAMANO_PAGINA, metavar="N",
                        help=f"publicaciones por peticion, entre 1 y {TAMANO_PAGINA} "
                             f"(por defecto {TAMANO_PAGINA}); bajarlo fuerza varias paginas")
    parser.add_argument("--salida", default=str(SALIDA_POR_DEFECTO), metavar="RUTA",
                        help=f"carpeta de destino (por defecto {SALIDA_POR_DEFECTO})")
    args = parser.parse_args(argv)
    if args.limite is not None and args.limite <= 0:
        parser.error("--limite debe ser un entero mayor que 0")
    if not 1 <= args.tamano_pagina <= TAMANO_PAGINA:
        parser.error(f"--tamano-pagina debe estar entre 1 y {TAMANO_PAGINA}")
    return args


def main(argv=None):
    args = parsear_argumentos(argv)
    configurar_logging()
    token, ig_id = cargar_credenciales()
    _SECRETOS.append(token)

    inicio = ahora_utc()
    reloj = time.monotonic()
    id_ejecucion = str(uuid.uuid4())
    cliente = ClienteGraph(token)
    descartadas_por_tipo = {}

    metadatos = {
        "id_ejecucion": id_ejecucion,
        "plataforma": PLATAFORMA,
        "version_api": VERSION_API,
        "fecha_hora_inicio_utc": iso(inicio),
        "fecha_hora_fin_utc": None,
        "duracion_segundos": None,
        "estado": None,
        "publicaciones_extraidas": 0,
        "llamadas_api": 0,
        "paginas_recorridas": 0,
        "error": None,
        "ig_business_account_id": ig_id,
        "parametros": {"limite": args.limite, "sin_insights": args.sin_insights,
                       "tamano_pagina": args.tamano_pagina},
        "metricas_descartadas": [],
        "x_business_use_case_usage": [],
    }
    respuestas = {
        "perfil": None,
        "paginas_media": [],
        "insights": None if args.sin_insights else {},
    }
    publicaciones = []

    log.info("Extraccion de Instagram %s | ejecucion %s", VERSION_API, id_ejecucion)
    try:
        log.info("[1/3] Perfil de la cuenta %s", ig_id)
        respuestas["perfil"] = cliente.get(ig_id, {"fields": CAMPOS_PERFIL})
        log.info("  @%s: %s seguidores, %s publicaciones",
                 respuestas["perfil"].get("username"),
                 respuestas["perfil"].get("followers_count"),
                 respuestas["perfil"].get("media_count"))

        log.info("[2/3] Publicaciones")
        publicaciones = extraer_publicaciones(cliente, ig_id, args.limite,
                                              respuestas["paginas_media"],
                                              args.tamano_pagina)

        if args.sin_insights:
            log.info("[3/3] Insights omitidos (--sin-insights)")
        else:
            log.info("[3/3] Insights de %d publicaciones", len(publicaciones))
            extraer_insights(cliente, publicaciones, respuestas["insights"],
                             descartadas_por_tipo)
        metadatos["estado"] = "EXITO"
    except TokenInvalido as exc:
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = exc.como_dict()
        log.error(MENSAJE_TOKEN)
    except ErrorExtraccion as exc:
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = exc.como_dict()
        log.error("Extraccion fallida: %s", exc)
    except KeyboardInterrupt:
        metadatos["estado"] = "INTERRUMPIDA"
        metadatos["error"] = {"tipo": "KeyboardInterrupt",
                              "mensaje": "Ejecucion interrumpida por el usuario."}
        log.warning("Ejecucion interrumpida por el usuario; se guardan los datos parciales.")
    except Exception as exc:  # error inesperado: igual se deja registro de auditoria
        metadatos["estado"] = "FALLIDA"
        metadatos["error"] = {"tipo": type(exc).__name__, "mensaje": ocultar_secretos(exc)}
        log.error("Error inesperado: %s: %s", type(exc).__name__, exc)

    # Si la ejecucion se corto a mitad de la paginacion, las publicaciones
    # parciales siguen estando en las paginas ya guardadas.
    if not publicaciones:
        publicaciones = [m for p in respuestas["paginas_media"] for m in (p.get("data") or [])]
        if args.limite is not None:
            publicaciones = publicaciones[:args.limite]

    fin = ahora_utc()
    metadatos.update({
        "fecha_hora_fin_utc": iso(fin),
        "duracion_segundos": round(time.monotonic() - reloj, 3),
        "publicaciones_extraidas": len(publicaciones),
        "llamadas_api": cliente.llamadas,
        "paginas_recorridas": len(respuestas["paginas_media"]),
        "metricas_descartadas": [
            {"tipo_publicacion": tipo, "metrica": metrica, "motivo": motivo}
            for tipo, metricas in descartadas_por_tipo.items()
            for metrica, motivo in metricas.items()
        ],
        "x_business_use_case_usage": cliente.historial_uso,
    })

    try:
        ruta = guardar({"metadatos_ejecucion": metadatos, "respuestas": respuestas},
                       args.salida, inicio, id_ejecucion)
    except OSError as exc:
        log.error("No se pudo guardar el archivo de salida: %s", exc)
        return 1

    log.info("Estado: %s | %d publicaciones | %d paginas | %d llamadas | %.1f s",
             metadatos["estado"], metadatos["publicaciones_extraidas"],
             metadatos["paginas_recorridas"], metadatos["llamadas_api"],
             metadatos["duracion_segundos"])
    log.info("Archivo guardado en %s", ruta)

    return {"EXITO": 0, "INTERRUMPIDA": 130}.get(metadatos["estado"], 1)


if __name__ == "__main__":
    sys.exit(main())
