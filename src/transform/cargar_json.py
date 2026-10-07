#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga de JSON crudos en DataFrames de Pandas
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Fase 3 (Transformacion), primer paso: leer los JSON que guardan los extractores
en data/raw/ y dejarlos como DataFrames "planos", uno por entidad del modelo.
Este modulo no limpia: solo lee, aplana y filtra columnas.

DataFrames que entrega cargar_instagram()
-----------------------------------------
    ejecucion      1 fila por archivo   (alimenta DIM_EJECUCION / linaje)
    perfil         0 o 1 fila           (0 si la ejecucion fallo antes de leerlo)
    publicaciones  1 fila por publicacion
    metricas       1 fila por (publicacion, metrica), formato largo

USO
---
    from src.transform.cargar_json import cargar_instagram, cargar_todo_instagram

    tablas = cargar_instagram("data/raw/instagram/instagram_..._UTC_xxxx.json")
    todas  = cargar_todo_instagram("data/raw/instagram")   # concatena todos los archivos

    python src/transform/cargar_json.py                    # resumen de lo que hay en data/raw/instagram
"""

import json
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    sys.exit("Falta la libreria 'pandas'. Instala con: pip install -r requirements.txt")

RAIZ = Path(__file__).resolve().parents[2]
CARPETA_INSTAGRAM = RAIZ / "data" / "raw" / "instagram"

# --------------------------------------------------------------------------
# Columnas que se conservan (lista blanca). Todo lo demas se descarta.
# --------------------------------------------------------------------------

COLUMNAS_EJECUCION = [
    "id_ejecucion", "plataforma", "version_api",
    "fecha_hora_inicio_utc", "fecha_hora_fin_utc", "duracion_segundos",
    "estado", "publicaciones_extraidas", "llamadas_api", "paginas_recorridas",
    "ig_business_account_id",
    "error_tipo", "error_codigo", "error_mensaje",
]

COLUMNAS_PERFIL = [
    "id_ejecucion", "id", "username", "name",
    "followers_count", "follows_count", "media_count",
]

COLUMNAS_PUBLICACION = [
    "id_ejecucion", "id", "caption", "media_type", "media_product_type",
    "permalink", "timestamp", "like_count", "comments_count",
]

COLUMNAS_METRICA = ["id_ejecucion", "id_publicacion", "metrica", "periodo", "valor"]


def _vacio(columnas):
    return pd.DataFrame(columns=columnas)


def _leer_json(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


# --------------------------------------------------------------------------
# Instagram
# --------------------------------------------------------------------------

def _tabla_ejecucion(meta):
    error = meta.get("error") or {}
    fila = {col: meta.get(col) for col in COLUMNAS_EJECUCION
            if not col.startswith("error_")}
    fila["error_tipo"] = error.get("tipo")
    fila["error_codigo"] = error.get("codigo")
    fila["error_mensaje"] = error.get("mensaje")
    return pd.DataFrame([fila], columns=COLUMNAS_EJECUCION)


def _tabla_perfil(perfil, id_ejecucion):
    # perfil es None cuando la ejecucion fallo antes de leerlo.
    if not perfil:
        return _vacio(COLUMNAS_PERFIL)
    fila = {col: perfil.get(col) for col in COLUMNAS_PERFIL if col != "id_ejecucion"}
    fila["id_ejecucion"] = id_ejecucion
    return pd.DataFrame([fila], columns=COLUMNAS_PERFIL)


def _tabla_publicaciones(paginas, id_ejecucion):
    filas = []
    for pagina in paginas or []:
        for media in pagina.get("data", []):
            fila = {col: media.get(col) for col in COLUMNAS_PUBLICACION
                    if col != "id_ejecucion"}
            fila["id_ejecucion"] = id_ejecucion
            filas.append(fila)
    if not filas:
        return _vacio(COLUMNAS_PUBLICACION)
    return pd.DataFrame(filas, columns=COLUMNAS_PUBLICACION)


def _tabla_metricas(insights, id_ejecucion):
    """
    insights = {media_id: {"respuesta": {"data": [{"name", "period",
    "values": [{"value": N}]}]}, "error": ...}}. Se aplana a formato largo.
    Las publicaciones sin insights (error o respuesta vacia) no generan filas:
    su motivo queda en el JSON crudo, no en la tabla de metricas.
    """
    filas = []
    for id_publicacion, registro in (insights or {}).items():
        respuesta = (registro or {}).get("respuesta") or {}
        for metrica in respuesta.get("data", []):
            valores = metrica.get("values") or [{}]
            filas.append({
                "id_ejecucion": id_ejecucion,
                "id_publicacion": id_publicacion,
                "metrica": metrica.get("name"),
                "periodo": metrica.get("period"),
                "valor": valores[0].get("value"),
            })
    if not filas:
        return _vacio(COLUMNAS_METRICA)
    return pd.DataFrame(filas, columns=COLUMNAS_METRICA)


def cargar_instagram(ruta):
    """Carga un JSON crudo de Instagram y devuelve un dict de DataFrames."""
    crudo = _leer_json(ruta)
    meta = crudo.get("metadatos_ejecucion") or {}
    respuestas = crudo.get("respuestas") or {}
    id_ejecucion = meta.get("id_ejecucion")

    return {
        "ejecucion": _tabla_ejecucion(meta),
        "perfil": _tabla_perfil(respuestas.get("perfil"), id_ejecucion),
        "publicaciones": _tabla_publicaciones(respuestas.get("paginas_media"), id_ejecucion),
        "metricas": _tabla_metricas(respuestas.get("insights"), id_ejecucion),
    }


def cargar_todo_instagram(carpeta=CARPETA_INSTAGRAM):
    """
    Carga todos los JSON de la carpeta y concatena cada tabla. Si no hay
    archivos, devuelve las cuatro tablas vacias con sus columnas.
    """
    archivos = sorted(Path(carpeta).glob("instagram_*.json"))
    columnas = {"ejecucion": COLUMNAS_EJECUCION, "perfil": COLUMNAS_PERFIL,
                "publicaciones": COLUMNAS_PUBLICACION, "metricas": COLUMNAS_METRICA}
    if not archivos:
        return {nombre: _vacio(cols) for nombre, cols in columnas.items()}

    cargados = [cargar_instagram(a) for a in archivos]
    resultado = {}
    for nombre, cols in columnas.items():
        partes = [c[nombre] for c in cargados if not c[nombre].empty]
        resultado[nombre] = (pd.concat(partes, ignore_index=True)
                             if partes else _vacio(cols))
    return resultado


# --------------------------------------------------------------------------
# Resumen por consola
# --------------------------------------------------------------------------

def main(argv=None):
    carpeta = Path(argv[0]) if argv else CARPETA_INSTAGRAM
    tablas = cargar_todo_instagram(carpeta)
    print(f"Carpeta: {carpeta}")
    for nombre, df in tablas.items():
        print(f"\n[{nombre}] {len(df)} fila(s), {len(df.columns)} columna(s)")
        if not df.empty:
            print(df.to_string(index=False, max_colwidth=40))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
