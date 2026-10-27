#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga de JSON crudos en DataFrames de Pandas
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Fase 3 (Transformacion), primer paso: leer los JSON que guardan los extractores
en data/raw/ y dejarlos como DataFrames "planos", uno por entidad del modelo.
Este modulo no limpia: solo lee, aplana y filtra columnas. Los campos anidados
se aplanan con el nombre de su ruta en el JSON (p. ej. "statistics.viewCount").

DataFrames que entrega cargar_instagram()
-----------------------------------------
    ejecucion      1 fila por archivo   (alimenta DIM_EJECUCION / linaje)
    perfil         0 o 1 fila           (0 si la ejecucion fallo antes de leerlo)
    publicaciones  1 fila por publicacion
    metricas       1 fila por (publicacion, metrica), formato largo

DataFrames que entrega cargar_youtube()
---------------------------------------
    ejecucion      1 fila por archivo
    canal          0 o 1 fila
    videos         1 fila por video del listado de uploads
    estadisticas   1 fila por video (vacia si se uso --sin-estadisticas)

USO
---
    from src.transform.cargar_json import cargar_instagram, cargar_todo_instagram
    from src.transform.cargar_json import cargar_youtube, cargar_todo_youtube

    tablas = cargar_instagram("data/raw/instagram/instagram_..._UTC_xxxx.json")
    todas  = cargar_todo_youtube("data/raw/youtube")       # concatena todos los archivos

    python src/transform/cargar_json.py                    # resumen de lo que hay en data/raw
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
CARPETA_YOUTUBE = RAIZ / "data" / "raw" / "youtube"

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

COLUMNAS_EJECUCION_YT = [
    "id_ejecucion", "plataforma", "version_api",
    "fecha_hora_inicio_utc", "fecha_hora_fin_utc", "duracion_segundos",
    "estado", "videos_extraidos", "llamadas_api", "paginas_recorridas",
    "cuota_estimada", "youtube_channel_id",
    "error_tipo", "error_codigo", "error_mensaje",
]

COLUMNAS_CANAL = [
    "id_ejecucion", "id", "snippet.title", "snippet.customUrl", "snippet.publishedAt",
    "statistics.subscriberCount", "statistics.videoCount", "statistics.viewCount",
]

COLUMNAS_VIDEO = [
    "id_ejecucion", "contentDetails.videoId", "snippet.title", "snippet.description",
    "contentDetails.videoPublishedAt", "status.privacyStatus",
]

COLUMNAS_ESTADISTICA = [
    "id_ejecucion", "id", "contentDetails.duration",
    "statistics.viewCount", "statistics.likeCount", "statistics.commentCount",
]


def _vacio(columnas):
    return pd.DataFrame(columns=columnas)


def _leer_json(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


def _obtener(objeto, ruta):
    """Lee un campo anidado por su ruta con puntos; None si falta algun nivel."""
    for parte in ruta.split("."):
        if not isinstance(objeto, dict):
            return None
        objeto = objeto.get(parte)
    return objeto


def _tabla(items, columnas, id_ejecucion):
    filas = []
    for item in items:
        fila = {col: _obtener(item, col) for col in columnas if col != "id_ejecucion"}
        fila["id_ejecucion"] = id_ejecucion
        filas.append(fila)
    if not filas:
        return _vacio(columnas)
    return pd.DataFrame(filas, columns=columnas)


def _items_de_paginas(paginas, clave):
    return [item for pagina in paginas or [] for item in (pagina or {}).get(clave) or []]


def _tabla_ejecucion(meta, columnas):
    error = meta.get("error") or {}
    fila = {col: meta.get(col) for col in columnas if not col.startswith("error_")}
    fila["error_tipo"] = error.get("tipo")
    # YouTube no siempre trae un codigo propio; ahi el codigo es el estado HTTP.
    fila["error_codigo"] = error.get("codigo") or error.get("estado_http")
    fila["error_mensaje"] = error.get("mensaje")
    return pd.DataFrame([fila], columns=columnas)


def _cargar_carpeta(carpeta, patron, cargar, columnas):
    """
    Carga todos los JSON de la carpeta y concatena cada tabla. Si no hay
    archivos, devuelve todas las tablas vacias con sus columnas.
    """
    cargados = [cargar(a) for a in sorted(Path(carpeta).glob(patron))]
    resultado = {}
    for nombre, cols in columnas.items():
        partes = [c[nombre] for c in cargados if not c[nombre].empty]
        resultado[nombre] = (pd.concat(partes, ignore_index=True)
                             if partes else _vacio(cols))
    return resultado


# --------------------------------------------------------------------------
# Instagram
# --------------------------------------------------------------------------

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
    # perfil es None cuando la ejecucion fallo antes de leerlo.
    perfil = respuestas.get("perfil")

    return {
        "ejecucion": _tabla_ejecucion(meta, COLUMNAS_EJECUCION),
        "perfil": _tabla([perfil] if perfil else [], COLUMNAS_PERFIL, id_ejecucion),
        "publicaciones": _tabla(_items_de_paginas(respuestas.get("paginas_media"), "data"),
                                COLUMNAS_PUBLICACION, id_ejecucion),
        "metricas": _tabla_metricas(respuestas.get("insights"), id_ejecucion),
    }


def cargar_todo_instagram(carpeta=CARPETA_INSTAGRAM):
    return _cargar_carpeta(carpeta, "instagram_*.json", cargar_instagram, {
        "ejecucion": COLUMNAS_EJECUCION, "perfil": COLUMNAS_PERFIL,
        "publicaciones": COLUMNAS_PUBLICACION, "metricas": COLUMNAS_METRICA})


# --------------------------------------------------------------------------
# YouTube
# --------------------------------------------------------------------------

def cargar_youtube(ruta):
    """Carga un JSON crudo de YouTube y devuelve un dict de DataFrames."""
    crudo = _leer_json(ruta)
    meta = crudo.get("metadatos_ejecucion") or {}
    respuestas = crudo.get("respuestas") or {}
    id_ejecucion = meta.get("id_ejecucion")

    return {
        "ejecucion": _tabla_ejecucion(meta, COLUMNAS_EJECUCION_YT),
        "canal": _tabla((respuestas.get("canal") or {}).get("items") or [],
                        COLUMNAS_CANAL, id_ejecucion),
        "videos": _tabla(_items_de_paginas(respuestas.get("paginas_playlist"), "items"),
                         COLUMNAS_VIDEO, id_ejecucion),
        "estadisticas": _tabla(_items_de_paginas(respuestas.get("estadisticas_videos"), "items"),
                               COLUMNAS_ESTADISTICA, id_ejecucion),
    }


def cargar_todo_youtube(carpeta=CARPETA_YOUTUBE):
    return _cargar_carpeta(carpeta, "youtube_*.json", cargar_youtube, {
        "ejecucion": COLUMNAS_EJECUCION_YT, "canal": COLUMNAS_CANAL,
        "videos": COLUMNAS_VIDEO, "estadisticas": COLUMNAS_ESTADISTICA})


# --------------------------------------------------------------------------
# Resumen por consola
# --------------------------------------------------------------------------

def _imprimir(titulo, tablas):
    print(f"\n===== {titulo} =====")
    for nombre, df in tablas.items():
        print(f"\n[{nombre}] {len(df)} fila(s), {len(df.columns)} columna(s)")
        if not df.empty:
            print(df.to_string(index=False, max_colwidth=40))


def main():
    _imprimir(f"Instagram ({CARPETA_INSTAGRAM})", cargar_todo_instagram())
    _imprimir(f"YouTube ({CARPETA_YOUTUBE})", cargar_todo_youtube())
    return 0


if __name__ == "__main__":
    sys.exit(main())
