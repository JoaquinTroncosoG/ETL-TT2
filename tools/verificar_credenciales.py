#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verificador de credenciales de API
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)
UTEM - Vicente Navarro / Joaquin Troncoso

Comprueba que las credenciales obtenidas en Google Cloud Console y en
Meta for Developers funcionan realmente contra sus APIs, y diagnostica
la causa concreta cuando algo falla.

USO
---
    pip install requests python-dotenv
    cp .env.example .env          # y rellenar los valores
    python verificar_credenciales.py

    # Canjear un token corto de Meta por uno de larga duracion (60 dias):
    python verificar_credenciales.py --canjear-token <TOKEN_CORTO>

Codigo de salida: 0 si todo pasa, 1 si hay al menos un fallo.
"""

import argparse
import datetime as dt
import os
import sys

try:
    import requests
except ImportError:
    sys.exit("Falta la libreria 'requests'.  Instala con:  pip install requests")

GRAPH = "https://graph.facebook.com/v21.0"
YT = "https://www.googleapis.com/youtube/v3"
TIMEOUT = 20

# --------------------------------------------------------------------------
# Utilidades de presentacion
# --------------------------------------------------------------------------

OK, FAIL, WARN, INFO = "  [OK]  ", " [FALLA]", " [AVISO]", "  [i]  "
_fallos = []


def titulo(texto):
    print("\n" + "=" * 72)
    print(texto)
    print("=" * 72)


def ok(msg):
    print(f"{OK} {msg}")


def falla(msg, arreglo=None):
    print(f"{FAIL} {msg}")
    if arreglo:
        print(f"         -> {arreglo}")
    _fallos.append(msg)


def aviso(msg):
    print(f"{WARN} {msg}")


def info(msg):
    print(f"{INFO} {msg}")


def cargar_env(ruta=".env"):
    """Carga el .env. Usa python-dotenv si esta disponible; si no, parsea a mano."""
    try:
        from dotenv import load_dotenv
        load_dotenv(ruta)
        return
    except ImportError:
        pass
    if not os.path.exists(ruta):
        return
    with open(ruta, encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, _, valor = linea.partition("=")
            os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


def get(clave):
    valor = os.environ.get(clave, "").strip()
    return valor or None


def enmascarar(valor):
    if not valor:
        return "(vacio)"
    if len(valor) <= 12:
        return valor[:3] + "..."
    return f"{valor[:6]}...{valor[-4:]}  ({len(valor)} caracteres)"


# --------------------------------------------------------------------------
# Google / YouTube
# --------------------------------------------------------------------------

def verificar_youtube():
    titulo("1. GOOGLE CLOUD CONSOLE  ->  YouTube Data API v3")

    api_key = get("YOUTUBE_API_KEY")
    canal = get("YOUTUBE_CHANNEL_ID")

    if not api_key:
        falla("YOUTUBE_API_KEY no esta definida en el .env",
              "Google Cloud Console -> APIs y servicios -> Credenciales -> Crear clave de API")
        return
    info(f"API Key detectada: {enmascarar(api_key)}")

    if not canal:
        aviso("YOUTUBE_CHANNEL_ID vacio. Se usara el canal de YouTube Developers "
              "solo para validar que la clave funciona.")
        canal = "UC_x5XG1OV2P6uZZ5FSM9Ttw"

    if not canal.startswith("UC") or len(canal) != 24:
        aviso(f"'{canal}' no parece un Channel ID valido "
              "(deben ser 24 caracteres que empiezan con 'UC'). "
              "Ojo: el @handle no sirve, se necesita el ID.")

    try:
        r = requests.get(
            f"{YT}/channels",
            params={"part": "snippet,statistics", "id": canal, "key": api_key},
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        falla(f"No hubo conexion con la API de YouTube: {exc}")
        return

    if r.status_code == 400:
        falla("La API Key es invalida o esta mal copiada (HTTP 400)",
              "Revisa que copiaste la clave completa, sin espacios.")
        return
    if r.status_code == 403:
        detalle = r.json().get("error", {}).get("errors", [{}])[0].get("reason", "")
        if detalle == "accessNotConfigured":
            falla("La YouTube Data API v3 no esta habilitada en este proyecto",
                  "Google Cloud Console -> Biblioteca -> YouTube Data API v3 -> Habilitar")
        elif detalle == "quotaExceeded":
            falla("Cuota diaria agotada (10.000 unidades)",
                  "Se reinicia a medianoche hora del Pacifico. Evita search.list (100 unidades).")
        else:
            falla(f"Acceso denegado por Google (HTTP 403, motivo: {detalle or 'desconocido'})",
                  "Revisa las restricciones de la API Key: deben permitir YouTube Data API v3.")
        return
    if r.status_code != 200:
        falla(f"Respuesta inesperada de YouTube (HTTP {r.status_code}): {r.text[:200]}")
        return

    items = r.json().get("items", [])
    if not items:
        falla(f"La clave funciona, pero el canal '{canal}' no existe o no es publico",
              "Verifica el Channel ID en YouTube Studio -> Configuracion -> Configuracion avanzada")
        return

    ch = items[0]
    stats = ch.get("statistics", {})
    ok("API Key valida y YouTube Data API v3 habilitada")
    ok(f"Canal accesible: '{ch['snippet']['title']}'")
    info(f"Suscriptores: {stats.get('subscriberCount', 'oculto')}  |  "
         f"Videos: {stats.get('videoCount', '?')}  |  "
         f"Vistas totales: {stats.get('viewCount', '?')}")

    # Prueba del recorrido eficiente en cuota: uploads playlist -> videos.list
    try:
        r2 = requests.get(
            f"{YT}/channels",
            params={"part": "contentDetails", "id": canal, "key": api_key},
            timeout=TIMEOUT,
        )
        uploads = (r2.json()["items"][0]["contentDetails"]
                   ["relatedPlaylists"]["uploads"])
        r3 = requests.get(
            f"{YT}/playlistItems",
            params={"part": "contentDetails", "playlistId": uploads,
                    "maxResults": 5, "key": api_key},
            timeout=TIMEOUT,
        )
        n = len(r3.json().get("items", []))
        ok(f"Playlist de subidas '{uploads}' legible ({n} videos en la primera pagina)")
        info("Esta es la ruta barata de extraccion: playlistItems.list (1 unidad) "
             "+ videos.list en lotes de 50 (1 unidad), en vez de search.list (100 unidades).")
    except Exception:
        aviso("No se pudo leer la playlist de subidas (no es bloqueante para esta tarea).")

    # OAuth (opcional, solo informativo)
    secreto = get("GOOGLE_CLIENT_SECRET_FILE") or "client_secret.json"
    if os.path.exists(secreto):
        ok(f"Archivo OAuth '{secreto}' presente (necesario para YouTube Analytics API)")
    else:
        aviso(f"No se encuentra '{secreto}'. Solo hace falta si vas a usar "
              "YouTube Analytics API (metricas privadas del canal propio).")


# --------------------------------------------------------------------------
# Meta / Instagram
# --------------------------------------------------------------------------

def canjear_token(token_corto):
    """Canjea un token de corta duracion por uno de larga duracion (60 dias)."""
    titulo("CANJE DE TOKEN META  ->  larga duracion (60 dias)")
    app_id, secreto = get("INSTAGRAM_APP_ID"), get("INSTAGRAM_APP_SECRET")
    if not app_id or not secreto:
        falla("Faltan INSTAGRAM_APP_ID o INSTAGRAM_APP_SECRET en el .env")
        return 1

    r = requests.get(f"{GRAPH}/oauth/access_token", params={
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": secreto,
        "fb_exchange_token": token_corto,
    }, timeout=TIMEOUT)

    if r.status_code != 200:
        falla(f"Meta rechazo el canje: {r.json().get('error', {}).get('message', r.text[:200])}",
              "Comprueba que el token corto sea reciente (dura ~1-2 horas) y de la MISMA app.")
        return 1

    datos = r.json()
    largo = datos["access_token"]
    segundos = datos.get("expires_in", 60 * 24 * 3600)
    vence = dt.date.today() + dt.timedelta(seconds=segundos)

    ok(f"Token de larga duracion obtenido (valido {segundos // 86400} dias)")
    print("\nCopia estas dos lineas a tu archivo .env:\n")
    print(f"INSTAGRAM_ACCESS_TOKEN={largo}")
    print(f"INSTAGRAM_TOKEN_EXPIRA={vence.isoformat()}\n")
    aviso(f"Agenda un recordatorio para renovarlo antes del {vence.isoformat()}.")
    return 0


def verificar_meta():
    titulo("2. META FOR DEVELOPERS  ->  Instagram Graph API")

    app_id = get("INSTAGRAM_APP_ID")
    secreto = get("INSTAGRAM_APP_SECRET")
    token = get("INSTAGRAM_ACCESS_TOKEN")

    if not app_id:
        falla("INSTAGRAM_APP_ID no esta definida",
              "Meta for Developers -> Configuracion de la app -> Basica")
    else:
        ok(f"App ID: {app_id}")

    if not secreto:
        falla("INSTAGRAM_APP_SECRET no esta definida",
              "Meta for Developers -> Configuracion de la app -> Basica -> Mostrar")
    else:
        ok(f"App Secret: {enmascarar(secreto)}")

    if not token:
        falla("INSTAGRAM_ACCESS_TOKEN no esta definida",
              "Genera un token en el Explorador de la API Graph y canjealo con:\n"
              "            python verificar_credenciales.py --canjear-token <TOKEN_CORTO>")
        return
    info(f"Access Token: {enmascarar(token)}")

    # --- 2.1 Estado del token -------------------------------------------------
    if app_id and secreto:
        r = requests.get(f"{GRAPH}/debug_token", params={
            "input_token": token,
            "access_token": f"{app_id}|{secreto}",
        }, timeout=TIMEOUT)
        if r.status_code == 200 and "data" in r.json():
            d = r.json()["data"]
            if not d.get("is_valid"):
                falla(f"El token NO es valido: {d.get('error', {}).get('message', '')}",
                      "Regeneralo en el Explorador de la API Graph y vuelve a canjearlo.")
                return
            exp = d.get("expires_at", 0)
            if exp == 0:
                ok("Token valido y SIN fecha de expiracion (System User Token)")
            else:
                vence = dt.datetime.fromtimestamp(exp)
                dias = (vence - dt.datetime.now()).days
                ok(f"Token valido. Expira el {vence:%d-%m-%Y} (quedan {dias} dias)")
                if dias < 7:
                    aviso("Quedan menos de 7 dias: es un token CORTO. Canjealo por uno de "
                          "60 dias con --canjear-token antes de seguir.")
            concedidos = set(d.get("scopes", []))
            requeridos = {
                "instagram_basic",
                "instagram_manage_insights",
                "pages_show_list",
                "pages_read_engagement",
            }
            faltantes = requeridos - concedidos
            if faltantes:
                falla(f"Al token le faltan permisos: {', '.join(sorted(faltantes))}",
                      "Regeneralo en el Explorador agregando esos permisos y "
                      "marcando la Pagina de Facebook en la ventana de autorizacion.")
            else:
                ok(f"Permisos correctos ({len(concedidos)} concedidos)")

    # --- 2.2 Paginas de Facebook ---------------------------------------------
    r = requests.get(f"{GRAPH}/me/accounts",
                     params={"access_token": token}, timeout=TIMEOUT)
    if r.status_code != 200:
        falla(f"Error consultando /me/accounts: "
              f"{r.json().get('error', {}).get('message', r.text[:200])}")
        return

    paginas = r.json().get("data", [])
    if not paginas:
        falla("El token no da acceso a ninguna Pagina de Facebook",
              "Al generar el token, en la ventana de autorizacion debes SELECCIONAR "
              "explicitamente la Pagina. Si no marcas nada, el token sale vacio.")
        return

    ok(f"Paginas accesibles: {len(paginas)}")
    for p in paginas:
        info(f"  - {p['name']}  (ID: {p['id']})")

    page_id = get("FB_PAGE_ID") or paginas[0]["id"]

    # --- 2.3 Cuenta de Instagram Business ------------------------------------
    r = requests.get(f"{GRAPH}/{page_id}", params={
        "fields": "name,instagram_business_account{id,username,followers_count,media_count}",
        "access_token": token,
    }, timeout=TIMEOUT)

    if r.status_code != 200:
        falla(f"Error consultando la Pagina {page_id}: "
              f"{r.json().get('error', {}).get('message', r.text[:200])}")
        return

    iga = r.json().get("instagram_business_account")
    if not iga:
        falla(f"La Pagina '{r.json().get('name', page_id)}' no tiene una cuenta de "
              "Instagram Business vinculada",
              "Causa casi segura: la cuenta de Instagram no esta en modo Business/Creator, "
              "o no esta vinculada a esta Pagina. Revisa la seccion 1 de la guia.")
        return

    ok(f"Instagram Business Account vinculada: @{iga.get('username', '?')}")
    info(f"  IG_BUSINESS_ACCOUNT_ID = {iga['id']}   <- copia esto a tu .env")
    info(f"  Seguidores: {iga.get('followers_count', '?')}  |  "
         f"Publicaciones: {iga.get('media_count', '?')}")

    # --- 2.4 Prueba real de insights -----------------------------------------
    r = requests.get(f"{GRAPH}/{iga['id']}/insights", params={
        "metric": "reach",
        "period": "day",
        "access_token": token,
    }, timeout=TIMEOUT)

    if r.status_code == 200 and r.json().get("data"):
        valores = r.json()["data"][0].get("values", [])
        ok("Endpoint de INSIGHTS operativo: se pueden extraer metricas de marketing")
        if valores:
            info(f"  Muestra -> alcance (reach): {valores[-1].get('value')} "
                 f"el {valores[-1].get('end_time', '')[:10]}")
    elif r.status_code == 429:
        aviso("HTTP 429: rate limit de Meta alcanzado (~200 llamadas/hora). "
              "Espera una hora. El pipeline debera implementar backoff exponencial.")
    else:
        msg = r.json().get("error", {}).get("message", r.text[:200])
        falla(f"Insights no accesible: {msg}",
              "Suele faltar el permiso 'instagram_manage_insights', o la cuenta "
              "tiene muy pocos seguidores para que Meta entregue metricas agregadas.")


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Verifica las credenciales de YouTube Data API e Instagram Graph API.")
    ap.add_argument("--canjear-token", metavar="TOKEN_CORTO",
                    help="Canjea un token corto de Meta por uno de larga duracion (60 dias)")
    ap.add_argument("--env", default=".env", help="Ruta al archivo .env (por defecto: .env)")
    args = ap.parse_args()

    cargar_env(args.env)

    print("\n" + "#" * 72)
    print("#  VERIFICADOR DE CREDENCIALES - Pipeline ETL Multi-Plataforma")
    print("#  UTEM - Trabajo de Titulo")
    print(f"#  Ejecutado: {dt.datetime.now():%d-%m-%Y %H:%M}")
    print("#" * 72)

    if args.canjear_token:
        return canjear_token(args.canjear_token)

    if not os.path.exists(args.env):
        aviso(f"No se encontro '{args.env}'. Crealo con:  cp .env.example .env")

    verificar_youtube()
    verificar_meta()

    titulo("RESUMEN")
    if _fallos:
        print(f"{len(_fallos)} punto(s) pendiente(s):\n")
        for i, f in enumerate(_fallos, 1):
            print(f"  {i}. {f}")
        print("\nRevisa la seccion correspondiente de GUIA_CREDENCIALES.md y vuelve a ejecutar.")
        return 1

    print("Todas las credenciales verificadas correctamente.")
    print("\nPuedes marcar la tarea de ClickUp como COMPLETADA.")
    print("Antes de cerrarla: guarda las capturas de pantalla de ambos portales,")
    print("son la evidencia del Capitulo de Implementacion del informe.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
