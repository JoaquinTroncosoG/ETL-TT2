#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard de monitoreo e integridad del pipeline ETL.

Muestra estado de las cargas, fallos, latencia, tiempos por etapa, volumen,
calidad de la transformacion, integridad del Data Warehouse y alertas.

Fuentes de datos (no consume Azure salvo con --azure o --integridad):
    - GitHub Actions: artefactos de la ejecucion diaria (por defecto si GITHUB_TOKEN esta en el .env).
      Se acumulan en resultados_monitoreo/historial_github/ porque GitHub los borra al expirar.
    - Local: data/processed/*/ejecucion.csv y logs/metricas_pipeline.csv
    - resultados_monitoreo/integridad.json  ultima auditoria guardada

USO
---
    python tools/dashboard_monitoreo.py --abrir
    python tools/dashboard_monitoreo.py --local        # solo ejecuciones hechas en este equipo
    python tools/dashboard_monitoreo.py --azure        # ejecuciones desde DIM_EJECUCION
    python tools/dashboard_monitoreo.py --integridad   # ejecuta la auditoria contra Azure y la guarda
"""

import argparse
import datetime as dt
import io
import json
import os
import sys
import webbrowser
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import dotenv_values

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

RUTA_PROCESSED = RAIZ / "data" / "processed"
RUTA_METRICAS = RAIZ / "logs" / "metricas_pipeline.csv"
CARPETA_SALIDA = RAIZ / "resultados_monitoreo"
RUTA_INTEGRIDAD = CARPETA_SALIDA / "integridad.json"
RUTA_PLANTILLA = Path(__file__).resolve().parent / "plantilla_dashboard.html"
CARPETA_GITHUB = CARPETA_SALIDA / "historial_github"
REPO_GITHUB = "JoaquinTroncosoG/ETL-TT2"
ARTEFACTO_GITHUB = "datos-procesados-etl"
ZONA = "America/Santiago"
HORAS_ATRASO = 26
DIAS_INTEGRIDAD_VIGENTE = 7
DIAS_AVISO_TOKEN = 15
PLATAFORMAS = ["youtube", "instagram"]
NOMBRES = {"youtube": "YouTube", "instagram": "Instagram"}

COLUMNAS = [
    "id_ejecucion", "plataforma", "fecha_hora_inicio_utc", "duracion_segundos",
    "estado", "contenidos_extraidos", "llamadas_api",
    "error_tipo", "error_codigo", "error_mensaje",
]


# --------------------------------------------------------------------------
# Lectura de datos
# --------------------------------------------------------------------------

def _normalizar_ejecucion(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={
        "videos_extraidos": "contenidos_extraidos",
        "publicaciones_extraidas": "contenidos_extraidos",
    })


def leer_local() -> pd.DataFrame:
    frames = []
    for plataforma in PLATAFORMAS:
        ruta = RUTA_PROCESSED / plataforma / "ejecucion.csv"
        if ruta.exists():
            frames.append(_normalizar_ejecucion(pd.read_csv(ruta, encoding="utf-8-sig")))
    if not frames:
        return pd.DataFrame(columns=COLUMNAS)
    return pd.concat(frames, ignore_index=True)


def leer_zip_artefacto(contenido: bytes):
    """Extrae ejecuciones y metricas por etapa de un artefacto de GitHub Actions."""
    ejecuciones, etapas = [], []
    with zipfile.ZipFile(io.BytesIO(contenido)) as z:
        for nombre in z.namelist():
            if any(nombre.endswith(f"{p}/ejecucion.csv") for p in PLATAFORMAS):
                ejecuciones.append(_normalizar_ejecucion(pd.read_csv(io.BytesIO(z.read(nombre)), encoding="utf-8-sig")))
            elif nombre.endswith("metricas_pipeline.csv"):
                etapas.append(pd.read_csv(io.BytesIO(z.read(nombre)), encoding="utf-8"))
    return ejecuciones, etapas


def _acumular(nuevos: list, ruta: Path, clave: str) -> pd.DataFrame:
    frames = [f for f in nuevos if not f.empty]
    if ruta.exists():
        frames.insert(0, pd.read_csv(ruta, encoding="utf-8"))
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=[clave], keep="last")
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(ruta, index=False, encoding="utf-8")
    return df


def sincronizar_github(token: str, carpeta: Path = CARPETA_GITHUB):
    """Descarga los artefactos vigentes de GitHub Actions y los suma al historial local."""
    cabeceras = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    url = f"https://api.github.com/repos/{REPO_GITHUB}/actions/artifacts"
    respuesta = requests.get(url, headers=cabeceras, params={"name": ARTEFACTO_GITHUB, "per_page": 100}, timeout=30)
    respuesta.raise_for_status()

    ejecuciones, etapas = [], []
    for artefacto in respuesta.json().get("artifacts", []):
        if artefacto.get("expired"):
            continue
        zip_resp = requests.get(artefacto["archive_download_url"], headers=cabeceras, timeout=60)
        zip_resp.raise_for_status()
        e, m = leer_zip_artefacto(zip_resp.content)
        ejecuciones += e
        etapas += m

    df = _acumular(ejecuciones, carpeta / "ejecuciones.csv", "id_ejecucion")
    df_etapas = _acumular(etapas, carpeta / "metricas_pipeline.csv", "fecha_inicio_utc")
    return df, df_etapas


def leer_azure(dias: int) -> pd.DataFrame:
    from sqlalchemy import text
    from src.load.cargar_datos import obtener_engine

    consulta = text("""
        SELECT id_ejecucion, id_plataforma AS plataforma, fecha_hora_inicio_utc,
               duracion_segundos, estado, contenidos_extraidos, llamadas_api,
               error_tipo, error_codigo, error_mensaje
        FROM dbo.DIM_EJECUCION
        WHERE fecha_hora_inicio_utc >= DATEADD(day, :desde, SYSUTCDATETIME())
    """)
    engine = obtener_engine()
    try:
        with engine.connect() as conn:
            return pd.read_sql(consulta, conn, params={"desde": -dias})
    finally:
        engine.dispose()


def leer_metricas_etapas(dias: int, ahora: pd.Timestamp, ruta: Path = RUTA_METRICAS) -> pd.DataFrame:
    if not ruta.exists():
        return pd.DataFrame()
    df = pd.read_csv(ruta, encoding="utf-8")
    df["inicio"] = pd.to_datetime(df["fecha_inicio_utc"], utc=True, errors="coerce", format="ISO8601").dt.tz_convert(ZONA)
    df = df.dropna(subset=["inicio"])
    return df[df["inicio"] >= ahora - pd.Timedelta(days=dias)].sort_values("inicio").reset_index(drop=True)


def ejecutar_integridad(ruta: Path = RUTA_INTEGRIDAD) -> dict:
    from tools.verificar_integridad import (
        auditar_conciliacion_volumen, auditar_consistencia_temporal, auditar_exactitud_metricas,
        auditar_fidelidad_atributos_publicacion, auditar_integridad_referencial,
        auditar_unicidad_y_deduplicacion, cargar_todos_los_raw, obtener_engine,
    )

    engine = obtener_engine()
    try:
        raw = cargar_todos_los_raw()
        ok_vol, volumen = auditar_conciliacion_volumen(engine, raw)
        pruebas = [
            ("Conciliacion de volumen y entidades", ok_vol),
            ("Fidelidad de atributos de publicacion", auditar_fidelidad_atributos_publicacion(engine, raw)),
            ("Exactitud de metricas cuantitativas", auditar_exactitud_metricas(engine, raw)),
            ("Integridad referencial (cero huerfanos)", auditar_integridad_referencial(engine)),
            ("Unicidad y deduplicacion", auditar_unicidad_y_deduplicacion(engine)),
            ("Consistencia temporal UTC", auditar_consistencia_temporal(engine)),
        ]
    finally:
        engine.dispose()

    resultado = {
        "fecha_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "pruebas": [{"nombre": n, "ok": bool(ok)} for n, ok in pruebas],
        "volumen": {k: int(v) for k, v in volumen.items()},
    }
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    return resultado


def leer_integridad(ruta: Path = RUTA_INTEGRIDAD):
    if not ruta.exists():
        return None
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def dias_token_instagram(ahora: pd.Timestamp):
    valor = (dotenv_values(RAIZ / ".env").get("INSTAGRAM_TOKEN_EXPIRA") or "").strip().strip("'\"")
    try:
        return (pd.Timestamp(valor).date() - ahora.date()).days
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Calculos
# --------------------------------------------------------------------------

def preparar(df: pd.DataFrame, dias: int, ahora: pd.Timestamp) -> pd.DataFrame:
    df = df.reindex(columns=COLUMNAS).copy()
    df["inicio"] = pd.to_datetime(df["fecha_hora_inicio_utc"], utc=True, errors="coerce", format="ISO8601").dt.tz_convert(ZONA)
    for col in ("duracion_segundos", "contenidos_extraidos", "llamadas_api", "error_codigo"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["estado"] = df["estado"].astype(str).str.strip().str.upper()
    df["plataforma"] = df["plataforma"].astype(str).str.strip().str.lower()
    df = df.dropna(subset=["inicio"]).drop_duplicates(subset=["id_ejecucion"])
    df = df[df["inicio"] >= ahora - pd.Timedelta(days=dias)]
    return df.sort_values("inicio").reset_index(drop=True)


def calcular_indicadores(df: pd.DataFrame, ahora: pd.Timestamp) -> dict:
    exitos = df[df["estado"] == "EXITO"]
    latencias = exitos["duracion_segundos"].dropna()
    total = len(df)

    por_plataforma = {}
    for plataforma in PLATAFORMAS:
        sub = df[df["plataforma"] == plataforma]
        if sub.empty:
            por_plataforma[plataforma] = None
            continue
        ultima = sub.iloc[-1]
        horas = (ahora - ultima["inicio"]).total_seconds() / 3600
        por_plataforma[plataforma] = {
            "ejecuciones": len(sub),
            "tasa_exito": (sub["estado"] == "EXITO").mean() * 100,
            "ultima_fecha": ultima["inicio"],
            "ultimo_estado": ultima["estado"],
            "ultimos_contenidos": ultima["contenidos_extraidos"],
            "horas_desde_ultima": horas,
            "atrasada": horas > HORAS_ATRASO,
        }

    fallos = df[df["estado"] != "EXITO"]
    ultimo_exito = exitos.groupby("plataforma")["inicio"].max()
    resueltos = fallos.apply(lambda f: f["plataforma"] in ultimo_exito and ultimo_exito[f["plataforma"]] > f["inicio"], axis=1) \
        if not fallos.empty else pd.Series(dtype=bool)
    return {
        "total": total,
        "exitos": len(exitos),
        "fallos": len(fallos),
        "tasa_exito": len(exitos) / total * 100 if total else 0.0,
        "latencia_prom": latencias.mean() if not latencias.empty else None,
        "latencia_p95": latencias.quantile(0.95) if not latencias.empty else None,
        "latencia_max": latencias.max() if not latencias.empty else None,
        "contenidos": df["contenidos_extraidos"].fillna(0).sum(),
        "llamadas": df["llamadas_api"].fillna(0).sum(),
        "por_plataforma": por_plataforma,
        "fallos_por_tipo": fallos["error_tipo"].fillna("Desconocido").value_counts(),
        "fallos_resueltos": resueltos,
        "fallos_pendientes": int((~resueltos).sum()),
    }


ERRORES_SIMPLES = {
    "TokenInvalido": "Venció el permiso de acceso a la cuenta y hay que renovarlo.",
    "ApiKeyInvalida": "La clave de acceso a la plataforma no es válida.",
    "ErrorApi": "La plataforma no entregó los datos solicitados.",
}


def explicar_error(tipo) -> str:
    return ERRORES_SIMPLES.get(tipo, "Ocurrió un error inesperado al descargar los datos.")


def hace_cuanto(horas: float) -> str:
    if horas < 1:
        return "menos de 1 hora"
    if horas < 48:
        return f"{horas:.0f} horas"
    return f"{horas / 24:.0f} días"


def construir_alertas(df, ind, etapas, integridad, dias_token, ahora) -> list:
    alertas = []
    for plataforma in PLATAFORMAS:
        nombre = NOMBRES[plataforma]
        info = ind["por_plataforma"][plataforma]
        pendientes = df[(df["plataforma"] == plataforma) & (df["estado"] != "EXITO")]
        pendientes = pendientes[~ind["fallos_resueltos"].reindex(pendientes.index, fill_value=False).astype(bool)]
        if not pendientes.empty:
            ultimo = pendientes.iloc[-1]
            alertas.append({"nivel": "critico", "titulo": f"{nombre} no se pudo actualizar",
                            "detalle": f"{explicar_error(ultimo['error_tipo'])} (último intento: {ultimo['inicio']:%d-%m-%Y %H:%M})"})
        if info is None:
            alertas.append({"nivel": "advertencia", "titulo": f"No hay datos recientes de {nombre}",
                            "detalle": "No se registraron actualizaciones en el periodo revisado."})
        elif info["atrasada"]:
            alertas.append({"nivel": "advertencia",
                            "titulo": f"{nombre} no se actualiza hace {hace_cuanto(info['horas_desde_ultima'])}",
                            "detalle": "Normalmente los datos se actualizan una vez al día."})

    if not etapas.empty:
        ultima = etapas.iloc[-1]
        nombres_etapa = {"extraccion": "descarga de datos", "transformacion": "limpieza de datos",
                         "carga": "guardado en la base de datos"}
        fallidas = [n for e, n in nombres_etapa.items() if ultima.get(f"estado_{e}") == "FALLIDA"]
        if fallidas:
            alertas.append({"nivel": "critico", "titulo": "La última actualización no terminó bien",
                            "detalle": "Falló: " + ", ".join(fallidas) + f" ({ultima['inicio']:%d-%m-%Y %H:%M})."})

    if integridad is None:
        alertas.append({"nivel": "info", "titulo": "Aún no se ha verificado la base de datos",
                        "detalle": "Se recomienda revisar que los datos guardados coincidan con los descargados."})
    else:
        fallidas = [p for p in integridad["pruebas"] if not p["ok"]]
        if fallidas:
            alertas.append({"nivel": "critico", "titulo": "Se encontraron diferencias en la base de datos",
                            "detalle": f"{len(fallidas)} de {len(integridad['pruebas'])} revisiones no pasaron."})
        antiguedad = (ahora - pd.Timestamp(integridad["fecha_utc"]).tz_convert(ZONA)).days
        if antiguedad > DIAS_INTEGRIDAD_VIGENTE:
            alertas.append({"nivel": "advertencia", "titulo": f"La base de datos no se revisa hace {antiguedad} días",
                            "detalle": "Se recomienda revisarla al menos una vez por semana."})

    if dias_token is not None and dias_token <= DIAS_AVISO_TOKEN:
        alertas.append({"nivel": "critico" if dias_token <= 0 else "advertencia",
                        "titulo": "Venció el permiso de acceso a Instagram" if dias_token <= 0
                        else f"El permiso de acceso a Instagram vence en {dias_token} días",
                        "detalle": "Hay que renovarlo para que Instagram se siga actualizando."})

    orden = {"critico": 0, "advertencia": 1, "info": 2}
    return sorted(alertas, key=lambda a: orden[a["nivel"]])


# --------------------------------------------------------------------------
# Serializacion para el HTML
# --------------------------------------------------------------------------

def _num(valor):
    if valor is None or pd.isna(valor):
        return None
    valor = float(valor)
    return int(valor) if valor.is_integer() else round(valor, 3)


def _texto(valor):
    return None if valor is None or pd.isna(valor) else str(valor)


def resumen_indicadores(ind: dict) -> dict:
    return {
        "total": ind["total"], "exitos": ind["exitos"], "fallos": ind["fallos"],
        "tasa_exito": round(ind["tasa_exito"], 1), "pendientes": ind["fallos_pendientes"],
        "latencia_prom": _num(ind["latencia_prom"]), "latencia_p95": _num(ind["latencia_p95"]),
        "latencia_max": _num(ind["latencia_max"]),
        "contenidos": _num(ind["contenidos"]), "llamadas": _num(ind["llamadas"]),
    }


def construir_datos(df_crudo, etapas_crudo, integridad, origen, dias, ahora, dias_token) -> dict:
    df = preparar(df_crudo, dias, ahora)
    ind = calcular_indicadores(df, ahora)


    ejecuciones = []
    for indice, f in df.iterrows():
        ejecuciones.append({
            "plataforma": f["plataforma"],
            "fecha": f"{f['inicio']:%d-%m-%Y %H:%M}",
            "dia": f"{f['inicio']:%Y-%m-%d}",
            "ts": int(f["inicio"].timestamp() * 1000),
            "duracion": _num(f["duracion_segundos"]),
            "estado": f["estado"],
            "contenidos": _num(f["contenidos_extraidos"]),
            "llamadas": _num(f["llamadas_api"]),
            "error_tipo": _texto(f["error_tipo"]),
            "error_codigo": _num(f["error_codigo"]),
            "error_mensaje": _texto(f["error_mensaje"]),
            "resuelto": None if f["estado"] == "EXITO" else bool(ind["fallos_resueltos"].get(indice, False)),
        })

    etapas = []
    for _, f in etapas_crudo.iterrows():
        originales, validos = _num(f.get("registros_originales")), _num(f.get("registros_validos"))
        etapas.append({
            "fecha": f"{f['inicio']:%d-%m %H:%M}",
            "plataforma": _texto(f.get("plataforma")),
            "extraccion": _num(f.get("seg_extraccion")),
            "transformacion": _num(f.get("seg_transformacion")),
            "carga": _num(f.get("seg_carga")),
            "total": _num(f.get("seg_total")),
            "estados": {e: _texto(f.get(f"estado_{e}")) for e in ("extraccion", "transformacion", "carga")},
            "originales": originales,
            "validos": validos,
            "pct_validos": round(validos / originales * 100, 1) if originales else None,
        })

    plataformas = {}
    for plataforma, info in ind["por_plataforma"].items():
        plataformas[plataforma] = None if info is None else {
            "ejecuciones": info["ejecuciones"],
            "tasa_exito": round(info["tasa_exito"], 1),
            "ultima_fecha": f"{info['ultima_fecha']:%d-%m-%Y %H:%M}",
            "ultimo_estado": info["ultimo_estado"],
            "ultimos_contenidos": _num(info["ultimos_contenidos"]),
            "horas": round(info["horas_desde_ultima"], 1),
            "atrasada": info["atrasada"],
        }

    integridad_html = None
    if integridad:
        integridad_html = dict(integridad)
        integridad_html["fecha"] = f"{pd.Timestamp(integridad['fecha_utc']).tz_convert(ZONA):%d-%m-%Y %H:%M}"

    rango = []
    if not df.empty:
        dia = max(df["inicio"].min(), ahora - pd.Timedelta(days=dias)).normalize()
        while dia <= ahora.normalize():
            rango.append({"iso": f"{dia:%Y-%m-%d}", "etiqueta": f"{dia:%d-%m}"})
            dia += pd.Timedelta(days=1)

    return {
        "generado": f"{ahora:%d-%m-%Y %H:%M}",
        "origen": origen,
        "dias": dias,
        "zona": ZONA,
        "horas_atraso": HORAS_ATRASO,
        "indicadores": resumen_indicadores(ind),
        "plataformas": plataformas,
        "ejecuciones": ejecuciones,
        "etapas": etapas,
        "integridad": integridad_html,
        "alertas": construir_alertas(df, ind, etapas_crudo, integridad, dias_token, ahora),
        "rango_dias": rango,
    }


def construir_html(datos: dict) -> str:
    carga = json.dumps(datos, ensure_ascii=False).replace("</", "<\\/")
    return RUTA_PLANTILLA.read_text(encoding="utf-8").replace("__DATOS__", carga)


def generar_dashboard(df_crudo, origen: str, dias: int, carpeta: Path = CARPETA_SALIDA,
                      etapas=None, integridad=None, dias_token=None) -> Path:
    ahora = pd.Timestamp.now(tz=ZONA)
    etapas = etapas if etapas is not None else pd.DataFrame()
    datos = construir_datos(df_crudo, etapas, integridad, origen, dias, ahora, dias_token)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta_html = carpeta / "dashboard.html"
    ruta_html.write_text(construir_html(datos), encoding="utf-8")
    return ruta_html


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Genera el dashboard HTML de monitoreo del pipeline ETL.")
    parser.add_argument("--dias", type=int, default=30, help="Periodo a mostrar en dias (por defecto 30)")
    fuente = parser.add_mutually_exclusive_group()
    fuente.add_argument("--local", action="store_true", help="Usar solo las ejecuciones hechas en este equipo")
    fuente.add_argument("--azure", action="store_true", help="Leer DIM_EJECUCION desde Azure")
    parser.add_argument("--integridad", action="store_true", help="Ejecutar la auditoria de integridad contra Azure y guardarla")
    parser.add_argument("--abrir", action="store_true", help="Abrir el dashboard en el navegador")
    args = parser.parse_args(argv)

    ahora = pd.Timestamp.now(tz=ZONA)
    token = (os.getenv("GITHUB_TOKEN") or dotenv_values(RAIZ / ".env").get("GITHUB_TOKEN") or "").strip().strip("'\"")
    df, etapas, origen = None, None, None

    if args.azure:
        try:
            df, origen = leer_azure(args.dias), "Azure SQL (DIM_EJECUCION)"
        except Exception as e:
            print(f"[AVISO] No se pudo leer Azure ({e}).")
    elif not args.local and token:
        try:
            df, _ = sincronizar_github(token)
            etapas = leer_metricas_etapas(args.dias, ahora, CARPETA_GITHUB / "metricas_pipeline.csv")
            origen = "GitHub Actions (ejecucion diaria)"
            print(f"[OK] Historial de GitHub Actions sincronizado: {len(df)} ejecuciones.")
        except Exception as e:
            print(f"[AVISO] No se pudo leer GitHub Actions ({e}).")
            if (CARPETA_GITHUB / "ejecuciones.csv").exists():
                df = pd.read_csv(CARPETA_GITHUB / "ejecuciones.csv", encoding="utf-8")
                etapas = leer_metricas_etapas(args.dias, ahora, CARPETA_GITHUB / "metricas_pipeline.csv")
                origen = "GitHub Actions (historial guardado)"

    if df is None:
        df, origen = leer_local(), "CSV locales (data/processed)"
        print("[INFO] Se usan las ejecuciones locales.")
    if etapas is None:
        etapas = leer_metricas_etapas(args.dias, ahora)

    integridad = None
    if args.integridad:
        try:
            integridad = ejecutar_integridad()
        except Exception as e:
            print(f"[AVISO] No se pudo ejecutar la auditoria de integridad ({e}). Se usa la ultima guardada.")
    if integridad is None:
        integridad = leer_integridad()

    ruta = generar_dashboard(df, origen, args.dias, etapas=etapas,
                             integridad=integridad, dias_token=dias_token_instagram(ahora))
    print(f"[OK] Dashboard generado: {ruta}")
    if args.abrir:
        webbrowser.open(ruta.as_uri())
    return 0




if __name__ == "__main__":
    sys.exit(main())
