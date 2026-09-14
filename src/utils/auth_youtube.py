"""
auth_youtube.py - Autenticación OAuth 2.0 para YouTube Analytics API
=====================================================================
Este script realiza el flujo completo de autorización OAuth 2.0
con Google para acceder a YouTube Analytics API y YouTube Data API v3.

Requisitos previos:
  1. Tener un proyecto en Google Cloud Console.
  2. Habilitar "YouTube Data API v3" y "YouTube Analytics API".
  3. Crear credenciales OAuth 2.0 (Tipo: Aplicación de escritorio).
  4. Descargar el archivo JSON de credenciales y guardarlo como
     "client_secret.json" en la raíz del proyecto (carpeta Proyecto/).

Uso:
  python -m src.utils.auth_youtube
"""

import os
import pickle
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv

# ── Configuración ──────────────────────────────────────────────────
load_dotenv()

# Directorio raíz del proyecto (dos niveles arriba de este archivo)
ROOT_DIR = Path(__file__).resolve().parent.parent.parent

# Ruta al archivo de credenciales descargado de Google Cloud Console
CLIENT_SECRET_FILE = ROOT_DIR / os.getenv("GOOGLE_CLIENT_SECRET_FILE", "client_secret.json")

# Ruta donde se guardará el token generado (NO se sube a GitHub)
TOKEN_FILE = ROOT_DIR / "youtube_token.pickle"

# Permisos (scopes) que solicitamos a Google
# - youtube.readonly       → Leer datos del canal (Data API v3)
# - yt-analytics.readonly  → Leer métricas de analytics
SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def obtener_credenciales_youtube():
    """
    Ejecuta el flujo OAuth 2.0 y retorna las credenciales autenticadas.

    - Si ya existe un token válido guardado, lo reutiliza.
    - Si el token expiró, lo refresca automáticamente.
    - Si no hay token, abre el navegador para autorizar.

    Returns:
        google.oauth2.credentials.Credentials: Credenciales autenticadas.
    """
    credenciales = None

    # ── Paso 1: Intentar cargar un token existente ─────────────────
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE, "rb") as token:
            credenciales = pickle.load(token)
        print("[INFO] Token existente cargado desde youtube_token.pickle")

    # ── Paso 2: Verificar si el token es válido o refrescarlo ──────
    if credenciales and credenciales.valid:
        print("[OK] Token vigente. No es necesario re-autenticar.")
        return credenciales

    if credenciales and credenciales.expired and credenciales.refresh_token:
        print("[INFO] Token expirado. Refrescando automáticamente...")
        try:
            credenciales.refresh(Request())
            _guardar_token(credenciales)
            print("[OK] Token refrescado exitosamente.")
            return credenciales
        except Exception as e:
            print(f"[WARN] No se pudo refrescar el token: {e}")
            print("[INFO] Se iniciará el flujo de autorización completo.")

    # ── Paso 3: Flujo de autorización completo ─────────────────────
    if not CLIENT_SECRET_FILE.exists():
        print("=" * 60)
        print("[ERROR] No se encontró el archivo de credenciales:")
        print(f"        {CLIENT_SECRET_FILE}")
        print()
        print("  Pasos para obtenerlo:")
        print("  1. Ve a https://console.cloud.google.com/apis/credentials")
        print("  2. Crea credenciales OAuth 2.0 (Tipo: App de escritorio)")
        print("  3. Descarga el JSON y guárdalo como 'client_secret.json'")
        print("     en la carpeta raíz del proyecto.")
        print("=" * 60)
        return None

    print("[INFO] Iniciando flujo de autorización OAuth 2.0...")
    print("[INFO] Se abrirá una ventana en tu navegador.")
    print("       Inicia sesión con la cuenta de Google del canal.")
    print()

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET_FILE),
        scopes=SCOPES,
    )

    # Abre el navegador y espera la autorización del usuario
    credenciales = flow.run_local_server(
        port=8080,
        prompt="consent",
        access_type="offline",  # Para obtener refresh_token
    )

    _guardar_token(credenciales)
    print("[OK] Autorización exitosa. Token guardado.")
    return credenciales


def _guardar_token(credenciales):
    """Guarda las credenciales en un archivo pickle para reutilizarlas."""
    with open(TOKEN_FILE, "wb") as token:
        pickle.dump(credenciales, token)
    print(f"[INFO] Token guardado en: {TOKEN_FILE}")


# ── Ejecución directa ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  GENERADOR DE TOKEN OAuth 2.0 - YouTube")
    print("=" * 60)
    print()

    creds = obtener_credenciales_youtube()

    if creds:
        print()
        print("─" * 60)
        print("  RESUMEN DEL TOKEN GENERADO")
        print("─" * 60)
        print(f"  Token válido  : {creds.valid}")
        print(f"  Expiración    : {creds.expiry}")
        print(f"  Refresh token : {'Sí' if creds.refresh_token else 'No'}")
        print(f"  Scopes        : {creds.scopes}")
        print(f"  Guardado en   : {TOKEN_FILE}")
        print("─" * 60)
    else:
        print("[ERROR] No se pudieron obtener las credenciales.")
