"""
auth_instagram.py - Autenticación OAuth 2.0 para Instagram Graph API
=====================================================================
Este script intercambia un token de corta duración por uno de larga
duración (60 días) usando la API de Meta.

Requisitos previos:
  1. Tener una app creada en Meta for Developers.
  2. Tener un token de corta duración (obtenido desde el Graph API
     Explorer: https://developers.facebook.com/tools/explorer/).
  3. Tener el App ID y App Secret en el archivo .env.

Flujo:
  1. El usuario pega su token corto (obtenido del Graph API Explorer).
  2. El script lo intercambia por un token de larga duración (60 días).
  3. El script descubre automáticamente el IG Business Account ID.
  4. Actualiza el archivo .env con el token y la fecha de expiración.

Uso:
  python -m src.utils.auth_instagram
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv, set_key

# ── Configuración ──────────────────────────────────────────────────
load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE = ROOT_DIR / ".env"

INSTAGRAM_APP_ID = os.getenv("INSTAGRAM_APP_ID")
INSTAGRAM_APP_SECRET = os.getenv("INSTAGRAM_APP_SECRET")

GRAPH_API_BASE = "https://graph.facebook.com/v21.0"


def intercambiar_token_largo(token_corto: str) -> dict:
    """
    Intercambia un token de corta duración por uno de larga duración.

    Args:
        token_corto: Token obtenido desde el Graph API Explorer.

    Returns:
        dict con 'access_token' (str) y 'expires_in' (int, segundos).
    """
    print("[INFO] Intercambiando token corto por token de larga duración...")

    url = f"{GRAPH_API_BASE}/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": INSTAGRAM_APP_ID,
        "client_secret": INSTAGRAM_APP_SECRET,
        "fb_exchange_token": token_corto,
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code != 200:
        print(f"[ERROR] Código HTTP: {response.status_code}")
        print(f"[ERROR] Respuesta: {response.text}")
        return None

    data = response.json()

    if "access_token" not in data:
        print(f"[ERROR] Respuesta inesperada: {data}")
        return None

    print("[OK] Token de larga duración obtenido exitosamente.")
    return data


def descubrir_cuentas_instagram(token_largo: str) -> dict:
    """
    Descubre automáticamente las Pages de Facebook y las cuentas
    de Instagram Business vinculadas al token.

    Args:
        token_largo: Token de larga duración.

    Returns:
        dict con 'fb_page_id', 'fb_page_name', 'ig_account_id', 'ig_username'.
    """
    print("[INFO] Descubriendo cuentas vinculadas...")

    # Paso 1: Obtener las Pages de Facebook del usuario
    url_pages = f"{GRAPH_API_BASE}/me/accounts"
    params = {"access_token": token_largo}

    response = requests.get(url_pages, params=params, timeout=30)

    if response.status_code != 200:
        print(f"[ERROR] No se pudieron obtener las Pages: {response.text}")
        return None

    pages = response.json().get("data", [])

    if not pages:
        print("[ERROR] No se encontraron Pages de Facebook vinculadas.")
        print("        Asegúrate de que tu app tiene permiso 'pages_show_list'.")
        return None

    # Usar la primera Page encontrada
    page = pages[0]
    fb_page_id = page["id"]
    fb_page_name = page["name"]
    print(f"[OK] Page encontrada: {fb_page_name} (ID: {fb_page_id})")

    # Paso 2: Obtener la cuenta de Instagram Business de esa Page
    url_ig = f"{GRAPH_API_BASE}/{fb_page_id}"
    params_ig = {
        "fields": "instagram_business_account",
        "access_token": token_largo,
    }

    response_ig = requests.get(url_ig, params=params_ig, timeout=30)

    if response_ig.status_code != 200:
        print(f"[ERROR] No se pudo consultar la Page: {response_ig.text}")
        return None

    ig_data = response_ig.json().get("instagram_business_account")

    if not ig_data:
        print("[ERROR] La Page no tiene una cuenta de Instagram Business vinculada.")
        return None

    ig_account_id = ig_data["id"]

    # Paso 3: Obtener el username de Instagram
    url_ig_info = f"{GRAPH_API_BASE}/{ig_account_id}"
    params_info = {"fields": "username", "access_token": token_largo}
    response_info = requests.get(url_ig_info, params=params_info, timeout=30)

    ig_username = "desconocido"
    if response_info.status_code == 200:
        ig_username = response_info.json().get("username", "desconocido")

    print(f"[OK] Instagram Business: @{ig_username} (ID: {ig_account_id})")

    return {
        "fb_page_id": fb_page_id,
        "fb_page_name": fb_page_name,
        "ig_account_id": ig_account_id,
        "ig_username": ig_username,
    }


def actualizar_env(token_largo: str, expira_en_segundos: int, ig_account_id: str, fb_page_id: str):
    """
    Actualiza el archivo .env con el token generado y los IDs descubiertos.
    """
    fecha_expiracion = (datetime.now() + timedelta(seconds=expira_en_segundos)).strftime("%Y-%m-%d")

    set_key(str(ENV_FILE), "INSTAGRAM_ACCESS_TOKEN", token_largo)
    set_key(str(ENV_FILE), "INSTAGRAM_TOKEN_EXPIRA", fecha_expiracion)
    set_key(str(ENV_FILE), "IG_BUSINESS_ACCOUNT_ID", ig_account_id)
    set_key(str(ENV_FILE), "FB_PAGE_ID", fb_page_id)

    print(f"[OK] Archivo .env actualizado correctamente.")
    print(f"     Token expira el: {fecha_expiracion}")


# ── Ejecución directa ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  GENERADOR DE TOKEN OAuth 2.0 - Instagram Graph API")
    print("=" * 60)
    print()

    # Validar que existan las credenciales de la app
    if not INSTAGRAM_APP_ID or not INSTAGRAM_APP_SECRET:
        print("[ERROR] Faltan INSTAGRAM_APP_ID o INSTAGRAM_APP_SECRET en .env")
        sys.exit(1)

    # Pedir el token corto al usuario
    print("  Para obtener el token corto:")
    print("  1. Ve a: https://developers.facebook.com/tools/explorer/")
    print("  2. Selecciona tu app en el menú superior.")
    print("  3. Haz clic en 'Generate Access Token'.")
    print("  4. Acepta los permisos solicitados.")
    print("  5. Copia el token generado y pégalo aquí abajo.")
    print()

    token_corto = input("  Pega tu token corto aquí: ").strip()

    if not token_corto:
        print("[ERROR] No se proporcionó un token.")
        sys.exit(1)

    print()

    # Paso 1: Intercambiar por token largo
    resultado = intercambiar_token_largo(token_corto)

    if not resultado:
        print("[ERROR] No se pudo obtener el token de larga duración.")
        sys.exit(1)

    token_largo = resultado["access_token"]
    expira_en = resultado.get("expires_in", 5184000)  # 60 días por defecto

    # Paso 2: Descubrir las cuentas vinculadas
    cuentas = descubrir_cuentas_instagram(token_largo)

    if not cuentas:
        print("[WARN] No se pudieron descubrir las cuentas automáticamente.")
        print("       El token se guardará pero sin los IDs de Instagram/Facebook.")
        actualizar_env(token_largo, expira_en, "", "")
    else:
        actualizar_env(token_largo, expira_en, cuentas["ig_account_id"], cuentas["fb_page_id"])

    # Resumen
    print()
    print("─" * 60)
    print("  RESUMEN DEL TOKEN GENERADO")
    print("─" * 60)
    print(f"  Token válido     : Sí")
    print(f"  Tipo             : Larga duración (60 días)")
    expira_fecha = (datetime.now() + timedelta(seconds=expira_en)).strftime("%Y-%m-%d")
    print(f"  Expira el        : {expira_fecha}")
    if cuentas:
        print(f"  Facebook Page    : {cuentas['fb_page_name']} ({cuentas['fb_page_id']})")
        print(f"  Instagram        : @{cuentas['ig_username']} ({cuentas['ig_account_id']})")
    print(f"  Guardado en      : .env")
    print("─" * 60)
