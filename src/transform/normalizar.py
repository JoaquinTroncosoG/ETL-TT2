#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Normalizacion de formatos
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Lleva las tablas de cargar_json.py a un esquema comun para ambas plataformas:
nombres en snake_case, metricas en Int64, texto en string y fechas en UTC.
Las fechas sin zona horaria se interpretan en America/Santiago.

Tablas de salida: ejecucion, cuenta, contenidos, metricas (Instagram)
y estadisticas (YouTube).

USO
---
    from src.transform.cargar_json import cargar_todo_instagram
    from src.transform.normalizar import normalizar_instagram

    tablas = normalizar_instagram(cargar_todo_instagram())
"""

import datetime as dt
import logging
import re

import pandas as pd

ZONA_LOCAL = "America/Santiago"
TIPO_FECHA = "datetime64[ns, UTC]"

log = logging.getLogger("normalizar")

COLUMNAS = {
    "ejecucion": [
        "id_ejecucion", "plataforma", "version_api",
        "fecha_hora_inicio_utc", "fecha_hora_fin_utc", "duracion_segundos",
        "estado", "contenidos_extraidos", "llamadas_api", "paginas_recorridas",
        "cuota_estimada", "id_cuenta", "error_tipo", "error_codigo", "error_mensaje",
    ],
    "cuenta": [
        "id_ejecucion", "plataforma", "id_cuenta", "usuario", "nombre",
        "fecha_hora_creacion_utc", "cantidad_seguidores", "cantidad_seguidos",
        "cantidad_contenidos", "cantidad_vistas",
    ],
    "contenidos": [
        "id_ejecucion", "plataforma", "id_contenido", "titulo", "texto",
        "tipo_medio", "tipo_producto", "url", "privacidad",
        "fecha_hora_publicacion_utc", "cantidad_me_gusta", "cantidad_comentarios",
    ],
    "metricas": ["id_ejecucion", "plataforma", "id_contenido", "metrica", "periodo", "valor"],
    "estadisticas": [
        "id_ejecucion", "plataforma", "id_contenido", "duracion_video_segundos",
        "cantidad_vistas", "cantidad_me_gusta", "cantidad_comentarios",
    ],
}

TIPOS = {
    "fecha_hora_inicio_utc": "fecha",
    "fecha_hora_fin_utc": "fecha",
    "fecha_hora_creacion_utc": "fecha",
    "fecha_hora_publicacion_utc": "fecha",
    "duracion_segundos": "decimal",
    "duracion_video_segundos": "duracion",
    "contenidos_extraidos": "entero",
    "llamadas_api": "entero",
    "paginas_recorridas": "entero",
    "cuota_estimada": "entero",
    "error_codigo": "entero",
    "cantidad_seguidores": "entero",
    "cantidad_seguidos": "entero",
    "cantidad_contenidos": "entero",
    "cantidad_vistas": "entero",
    "cantidad_me_gusta": "entero",
    "cantidad_comentarios": "entero",
    "valor": "entero",
}

# (tabla de origen, tabla de destino, {columna de origen: columna de destino})
MAPEO_INSTAGRAM = [
    ("ejecucion", "ejecucion", {
        "publicaciones_extraidas": "contenidos_extraidos",
        "ig_business_account_id": "id_cuenta",
    }),
    ("perfil", "cuenta", {
        "id": "id_cuenta",
        "username": "usuario",
        "name": "nombre",
        "followers_count": "cantidad_seguidores",
        "follows_count": "cantidad_seguidos",
        "media_count": "cantidad_contenidos",
    }),
    ("publicaciones", "contenidos", {
        "id": "id_contenido",
        "caption": "texto",
        "media_type": "tipo_medio",
        "media_product_type": "tipo_producto",
        "permalink": "url",
        "timestamp": "fecha_hora_publicacion_utc",
        "like_count": "cantidad_me_gusta",
        "comments_count": "cantidad_comentarios",
    }),
    ("metricas", "metricas", {"id_publicacion": "id_contenido"}),
]

MAPEO_YOUTUBE = [
    ("ejecucion", "ejecucion", {
        "videos_extraidos": "contenidos_extraidos",
        "youtube_channel_id": "id_cuenta",
    }),
    ("canal", "cuenta", {
        "id": "id_cuenta",
        "snippet.title": "nombre",
        "snippet.customUrl": "usuario",
        "snippet.publishedAt": "fecha_hora_creacion_utc",
        "statistics.subscriberCount": "cantidad_seguidores",
        "statistics.videoCount": "cantidad_contenidos",
        "statistics.viewCount": "cantidad_vistas",
    }),
    ("videos", "contenidos", {
        "contentDetails.videoId": "id_contenido",
        "snippet.title": "titulo",
        "snippet.description": "texto",
        "contentDetails.videoPublishedAt": "fecha_hora_publicacion_utc",
        "status.privacyStatus": "privacidad",
    }),
    ("estadisticas", "estadisticas", {
        "id": "id_contenido",
        "contentDetails.duration": "duracion_video_segundos",
        "statistics.viewCount": "cantidad_vistas",
        "statistics.likeCount": "cantidad_me_gusta",
        "statistics.commentCount": "cantidad_comentarios",
    }),
]

_DURACION_ISO = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def a_snake_case(nombre):
    nombre = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(nombre))
    return re.sub(r"[^0-9a-zA-Z]+", "_", nombre).strip("_").lower()


def _a_utc(valor, zona):
    if isinstance(valor, str):
        valor = valor.strip()
    elif not isinstance(valor, dt.datetime):
        return pd.NaT
    try:
        momento = pd.Timestamp(valor)
    except (ValueError, TypeError):
        return pd.NaT
    if pd.isna(momento):
        return pd.NaT
    if momento.tzinfo is None:
        momento = momento.tz_localize(zona, ambiguous=True, nonexistent="shift_forward")
    return momento.tz_convert("UTC")


def a_utc(serie, zona=ZONA_LOCAL):
    """Fechas con offset se convierten a UTC; sin zona se asumen en `zona`."""
    convertida = pd.Series([_a_utc(v, zona) for v in serie], index=serie.index, dtype=object)
    convertida = pd.to_datetime(convertida, utc=True).astype(TIPO_FECHA)
    invalidas = int((convertida.isna() & serie.notna()).sum())
    if invalidas:
        log.warning("%s: %d fecha(s) no reconocida(s) quedan como NaT", serie.name, invalidas)
    return convertida


def a_entero(serie):
    """Convierte a Int64; lo vacio, no numerico o con decimales queda como <NA>."""
    texto = serie.astype("string").str.strip()
    numeros = pd.to_numeric(texto, errors="coerce").astype("Float64")
    numeros = numeros.where((numeros % 1 == 0).fillna(False))
    invalidos = int((numeros.isna() & texto.fillna("").ne("")).sum())
    if invalidos:
        log.warning("%s: %d valor(es) no entero(s) quedan como <NA>", serie.name, invalidos)
    return numeros.astype("Int64")


def _segundos_iso(valor):
    if not isinstance(valor, str):
        return valor
    partes = _DURACION_ISO.match(valor.strip())
    if not partes or valor.strip() in ("P", "PT"):
        return None
    dias, horas, minutos, segundos = (int(p or 0) for p in partes.groups())
    return ((dias * 24 + horas) * 60 + minutos) * 60 + segundos


def duracion_a_segundos(serie):
    return a_entero(serie.map(_segundos_iso).astype(object))


def _normalizar_tabla(df, mapeo, columnas, plataforma):
    df = df.rename(columns=mapeo)
    df = df.rename(columns={c: a_snake_case(c) for c in df.columns if c not in columnas})
    df["plataforma"] = plataforma
    extras = [c for c in df.columns if c not in columnas]
    df = df.reindex(columns=columnas + extras)

    for columna in df.columns:
        tipo = TIPOS.get(columna)
        if tipo == "fecha":
            df[columna] = a_utc(df[columna])
        elif tipo == "entero":
            df[columna] = a_entero(df[columna])
        elif tipo == "decimal":
            df[columna] = pd.to_numeric(df[columna], errors="coerce").astype("Float64")
        elif tipo == "duracion":
            df[columna] = duracion_a_segundos(df[columna])
        else:
            df[columna] = df[columna].astype("string")
    return df.reset_index(drop=True)


def _normalizar(tablas, mapeos, plataforma):
    return {
        destino: _normalizar_tabla(tablas[origen], mapeo, COLUMNAS[destino], plataforma)
        for origen, destino, mapeo in mapeos
        if origen in tablas
    }


def normalizar_instagram(tablas):
    return _normalizar(tablas, MAPEO_INSTAGRAM, "instagram")


def normalizar_youtube(tablas):
    return _normalizar(tablas, MAPEO_YOUTUBE, "youtube")
