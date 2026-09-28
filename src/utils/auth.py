#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modulo centralizado de autenticacion OAuth 2.0
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)
UTEM - Vicente Navarro / Joaquin Troncoso

Gestiona la obtencion, renovacion y validacion de tokens para las APIs
de YouTube (OAuth 2.0 con refresh token) e Instagram (token de larga
duracion de Meta, 60 dias).

Los scripts de extraccion importan este modulo en vez de gestionar
credenciales por su cuenta, centralizando la logica de renovacion.

USO COMO MODULO
---------------
    from src.utils.auth import obtener_credenciales_youtube, obtener_token_instagram

    # YouTube: devuelve un objeto Credentials listo para usar
    creds = obtener_credenciales_youtube()

    # Instagram: devuelve el token validado (str)
    token = obtener_token_instagram()

USO DIRECTO (diagnostico / renovacion manual)
---------------------------------------------
    python -m src.utils.auth                          # estado de ambas credenciales
    python -m src.utils.auth --renovar-youtube        # fuerza re-autorizacion YouTube
    python -m src.utils.auth --canjear-instagram TKN  # canjea token corto de Meta

Variables requeridas en el .env:
    YouTube:   YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID, GOOGLE_CLIENT_SECRET_FILE (opcional)
    Instagram: INSTAGRAM_APP_ID, INSTAGRAM_APP_SECRET, INSTAGRAM_ACCESS_TOKEN
"""

import argparse
import datetime as dt
import json
import logging
import os
import pickle
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Falta la libreria 'requests'. Instala con: pip install -r requirements.txt")

try:
    from dotenv import load_dotenv, set_key
except ImportError:
    sys.exit("Falta la libreria 'python-dotenv'. Instala con: pip install -r requirements.txt")

# --------------------------------------------------------------------------
# Configuracion
# --------------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parents[2]
ENV_PATH = RAIZ / ".env"
PICKLE_PATH = RAIZ / "youtube_token.pickle"
GRAPH = "https://graph.facebook.com/v21.0"
TIMEOUT = 20

# Dias de anticipacion para avisar que el token de Instagram esta por vencer
DIAS_AVISO_INSTAGRAM = 7

log = logging.getLogger("auth")


def _cargar_env():
    """Carga el .env desde la raiz del proyecto."""
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)


def _get(clave):
    """Obtiene una variable de entorno limpia."""
    return (os.getenv(clave) or "").strip() or None


# --------------------------------------------------------------------------
# YouTube OAuth 2.0 (google-auth / google-auth-oauthlib)
# --------------------------------------------------------------------------

SCOPES_YOUTUBE = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def _cargar_pickle():
    """Carga las credenciales OAuth guardadas en el pickle."""
    if not PICKLE_PATH.exists():
        return None
    try:
        with open(PICKLE_PATH, "rb") as f:
            return pickle.load(f)
    except Exception as exc:
        log.warning("No se pudo leer %s: %s", PICKLE_PATH, exc)
        return None


def _guardar_pickle(creds):
    """Persiste las credenciales OAuth en el pickle."""
    with open(PICKLE_PATH, "wb") as f:
        pickle.dump(creds, f)
    log.info("Token de YouTube guardado en %s", PICKLE_PATH)


def _refrescar_youtube(creds):
    """Intenta refrescar el token de YouTube usando el refresh_token."""
    try:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
        _guardar_pickle(creds)
        log.info("Token de YouTube renovado automaticamente (refresh_token).")
        return creds
    except Exception as exc:
        log.warning("No se pudo refrescar el token de YouTube: %s", exc)
        return None


def _autorizar_youtube():
    """Ejecuta el flujo completo de autorizacion OAuth 2.0 (abre navegador)."""
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        log.error("Falta 'google-auth-oauthlib'. Instala con: pip install google-auth-oauthlib")
        return None

    secret_file = _get("GOOGLE_CLIENT_SECRET_FILE") or "client_secret.json"
    secret_path = RAIZ / secret_file

    if not secret_path.exists():
        log.error("No se encuentra '%s'. Descargalo desde Google Cloud Console "
                  "(APIs y servicios -> Credenciales -> OAuth 2.0 -> Descargar JSON).",
                  secret_path)
        return None

    flow = InstalledAppFlow.from_client_secrets_file(str(secret_path), scopes=SCOPES_YOUTUBE)
    creds = flow.run_local_server(port=8080, prompt="consent",
                                  access_type="offline", open_browser=False)
    _guardar_pickle(creds)
    return creds


def obtener_credenciales_youtube(forzar_renovacion=False):
    """
    Devuelve un objeto google.oauth2.credentials.Credentials listo para usar.

    Flujo de decision:
        1. Si existe el pickle y el token es valido -> lo devuelve.
        2. Si el token esta expirado y hay refresh_token -> lo renueva automaticamente.
        3. Si no hay pickle o falla el refresh -> ejecuta el flujo OAuth completo.

    Si `forzar_renovacion` es True, ignora el pickle y re-autoriza.
    """
    _cargar_env()

    if not forzar_renovacion:
        creds = _cargar_pickle()
        if creds:
            if creds.valid:
                log.debug("Token de YouTube valido (no expirado).")
                return creds
            if creds.expired and creds.refresh_token:
                log.info("Token de YouTube expirado. Intentando renovar...")
                renovado = _refrescar_youtube(creds)
                if renovado:
                    return renovado

    log.info("Se requiere autorizacion manual de YouTube (se abrira el navegador).")
    return _autorizar_youtube()


def estado_youtube():
    """Devuelve un dict con el estado actual de las credenciales de YouTube."""
    creds = _cargar_pickle()
    if not creds:
        return {"estado": "SIN_TOKEN", "mensaje": "No existe youtube_token.pickle"}

    info = {
        "estado": "VALIDO" if creds.valid else "EXPIRADO",
        "tiene_refresh_token": bool(creds.refresh_token),
        "scopes": list(creds.scopes) if creds.scopes else [],
    }

    if hasattr(creds, "expiry") and creds.expiry:
        info["expira"] = creds.expiry.isoformat()
        restante = creds.expiry - dt.datetime.utcnow()
        info["dias_restantes"] = max(0, restante.days)
        info["renovacion_automatica"] = bool(creds.refresh_token)
    return info


# --------------------------------------------------------------------------
# Instagram / Meta (token de larga duracion, 60 dias)
# --------------------------------------------------------------------------

def _canjear_token_instagram(token_corto):
    """
    Canjea un token de corta duracion por uno de larga duracion (60 dias).
    Actualiza automaticamente el .env con el nuevo token y su fecha de expiracion.
    """
    app_id = _get("INSTAGRAM_APP_ID")
    app_secret = _get("INSTAGRAM_APP_SECRET")

    if not app_id or not app_secret:
        log.error("Faltan INSTAGRAM_APP_ID o INSTAGRAM_APP_SECRET en el .env")
        return None

    resp = requests.get(f"{GRAPH}/oauth/access_token", params={
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": token_corto,
    }, timeout=TIMEOUT)

    if resp.status_code != 200:
        error_msg = resp.json().get("error", {}).get("message", resp.text[:200])
        log.error("Meta rechazo el canje: %s", error_msg)
        return None

    datos = resp.json()
    token_largo = datos["access_token"]
    segundos = datos.get("expires_in", 60 * 24 * 3600)
    vence = dt.date.today() + dt.timedelta(seconds=segundos)

    # Actualizar .env automaticamente
    env_str = str(ENV_PATH)
    set_key(env_str, "INSTAGRAM_ACCESS_TOKEN", token_largo)
    set_key(env_str, "INSTAGRAM_TOKEN_EXPIRA", vence.isoformat())

    log.info("Token de Instagram canjeado exitosamente. Valido hasta %s (%d dias).",
             vence.isoformat(), segundos // 86400)
    return token_largo


def _validar_token_instagram(token):
    """Verifica el estado del token contra la API de debug de Meta."""
    app_id = _get("INSTAGRAM_APP_ID")
    app_secret = _get("INSTAGRAM_APP_SECRET")
    if not app_id or not app_secret:
        return {"estado": "NO_VERIFICABLE", "mensaje": "Faltan APP_ID o APP_SECRET"}

    try:
        resp = requests.get(f"{GRAPH}/debug_token", params={
            "input_token": token,
            "access_token": f"{app_id}|{app_secret}",
        }, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return {"estado": "ERROR_RED", "mensaje": str(exc)}

    if resp.status_code != 200 or "data" not in resp.json():
        return {"estado": "ERROR_API", "mensaje": resp.text[:200]}

    data = resp.json()["data"]
    if not data.get("is_valid"):
        return {
            "estado": "INVALIDO",
            "mensaje": data.get("error", {}).get("message", "Token invalido o revocado"),
        }

    resultado = {"estado": "VALIDO", "permisos": data.get("scopes", [])}
    exp = data.get("expires_at", 0)
    if exp > 0:
        vence = dt.datetime.fromtimestamp(exp)
        dias = (vence - dt.datetime.now()).days
        resultado["expira"] = vence.isoformat()
        resultado["dias_restantes"] = max(0, dias)

        if dias < DIAS_AVISO_INSTAGRAM:
            resultado["estado"] = "POR_VENCER"
            resultado["mensaje"] = (
                f"El token vence en {dias} dia(s) ({vence:%d-%m-%Y}). "
                f"Regenera un token corto en el Graph API Explorer y ejecuta: "
                f"python -m src.utils.auth --canjear-instagram <TOKEN_CORTO>"
            )
            log.warning(resultado["mensaje"])

    return resultado


def obtener_token_instagram():
    """
    Devuelve el token de Instagram validado (str).

    Flujo de decision:
        1. Carga el token del .env.
        2. Lo valida contra la API de Meta.
        3. Si esta por vencer (< 7 dias), lanza un warning.
        4. Si es invalido, lanza un error.
    """
    _cargar_env()
    token = _get("INSTAGRAM_ACCESS_TOKEN")

    if not token:
        log.error("INSTAGRAM_ACCESS_TOKEN no esta definida en el .env")
        return None

    estado = _validar_token_instagram(token)

    if estado["estado"] == "INVALIDO":
        log.error("Token de Instagram invalido: %s", estado.get("mensaje"))
        log.error("Genera un token nuevo en el Graph API Explorer y ejecuta: "
                  "python -m src.utils.auth --canjear-instagram <TOKEN_CORTO>")
        return None

    if estado["estado"] == "POR_VENCER":
        # Warning ya emitido dentro de _validar_token_instagram
        pass

    if estado["estado"] in ("VALIDO", "POR_VENCER", "NO_VERIFICABLE"):
        return token

    log.warning("No se pudo verificar el token de Instagram: %s", estado.get("mensaje"))
    return token  # Devuelve igual; el script de extraccion lo intentara


def estado_instagram():
    """Devuelve un dict con el estado actual del token de Instagram."""
    _cargar_env()
    token = _get("INSTAGRAM_ACCESS_TOKEN")
    if not token:
        return {"estado": "SIN_TOKEN", "mensaje": "INSTAGRAM_ACCESS_TOKEN no definida"}
    return _validar_token_instagram(token)


# --------------------------------------------------------------------------
# CLI: diagnostico y renovacion manual
# --------------------------------------------------------------------------

def _imprimir_estado(nombre, estado):
    """Imprime el estado de una credencial de forma legible."""
    icono = {"VALIDO": "[OK]", "POR_VENCER": "[!!]", "EXPIRADO": "[XX]",
             "INVALIDO": "[XX]", "SIN_TOKEN": "[--]"}.get(estado.get("estado"), "[??]")
    print(f"\n  {icono} {nombre}: {estado.get('estado', 'DESCONOCIDO')}")

    for clave, valor in estado.items():
        if clave == "estado":
            continue
        print(f"        {clave}: {valor}")


def main():
    parser = argparse.ArgumentParser(
        description="Gestiona la autenticacion OAuth 2.0 del pipeline ETL.")
    parser.add_argument("--renovar-youtube", action="store_true",
                        help="Fuerza re-autorizacion OAuth 2.0 de YouTube (abre navegador)")
    parser.add_argument("--canjear-instagram", metavar="TOKEN_CORTO",
                        help="Canjea un token corto de Meta por uno de larga duracion (60 dias)")
    parser.add_argument("--estado", action="store_true",
                        help="Muestra el estado actual de todas las credenciales")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    _cargar_env()

    print("\n" + "=" * 60)
    print("  GESTOR DE AUTENTICACION OAuth 2.0 - Pipeline ETL")
    print(f"  Ejecutado: {dt.datetime.now():%d-%m-%Y %H:%M}")
    print("=" * 60)

    if args.canjear_instagram:
        token = _canjear_token_instagram(args.canjear_instagram)
        if token:
            print("\n  [OK] Token de Instagram actualizado en .env")
            return 0
        return 1

    if args.renovar_youtube:
        creds = obtener_credenciales_youtube(forzar_renovacion=True)
        if creds:
            print("\n  [OK] Credenciales de YouTube renovadas")
            return 0
        return 1

    # Por defecto o con --estado: mostrar diagnostico
    print("\n  --- YouTube (OAuth 2.0) ---")
    _imprimir_estado("YouTube", estado_youtube())
    print("\n  --- Instagram (Token Meta) ---")
    _imprimir_estado("Instagram", estado_instagram())
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
