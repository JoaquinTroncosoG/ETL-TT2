#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de JSON sinteticos para pruebas de carga (stress test)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Escribe archivos con la misma estructura que guardan extract_youtube.py y
extract_instagram.py en data/raw/, para pasar volumen por el pipeline sin
gastar cuota de las APIs. Todos los identificadores llevan el prefijo
PREFIJO ("STRESS-") para poder borrarlos de Azure SQL al terminar.

USO
---
    from tools.generar_sinteticos import generar_youtube, generar_instagram

    generar_youtube(500, "carpeta/youtube")
    generar_instagram(500, "carpeta/instagram")
"""

import datetime as dt
import json
import random
import uuid
from pathlib import Path

PREFIJO = "STRESS-"
TAMANO_PAGINA = 50

TIPOS_IG = [("IMAGE", "FEED"), ("VIDEO", "REELS"), ("CAROUSEL_ALBUM", "FEED")]


def _iso(momento):
    return momento.isoformat(timespec="seconds")


def _metadatos(plataforma, version_api, inicio, llamadas, paginas):
    fin = inicio + dt.timedelta(seconds=1)
    return {
        "id_ejecucion": f"{PREFIJO}{uuid.uuid4()}",
        "plataforma": plataforma,
        "version_api": version_api,
        "fecha_hora_inicio_utc": _iso(inicio),
        "fecha_hora_fin_utc": _iso(fin),
        "duracion_segundos": 1.0,
        "estado": "EXITO",
        "llamadas_api": llamadas,
        "paginas_recorridas": paginas,
        "error": None,
    }


def _paginas(items, n_paginas):
    return [items[i * TAMANO_PAGINA:(i + 1) * TAMANO_PAGINA] for i in range(n_paginas)]


def _guardar(datos, carpeta, plataforma, id_ejecucion):
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{plataforma}_stress_{id_ejecucion[len(PREFIJO):][:8]}.json"
    ruta.write_text(json.dumps(datos, ensure_ascii=False), encoding="utf-8")
    return ruta


def generar_youtube(n, carpeta, semilla=None):
    """JSON de YouTube Data API v3 con n videos (playlistItems + videos.list)."""
    rnd = random.Random(semilla)
    ahora = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    n_paginas = -(-n // TAMANO_PAGINA)
    meta = _metadatos("youtube", "v3", ahora, 2 + 2 * n_paginas, n_paginas)
    clave = meta["id_ejecucion"][len(PREFIJO):][:8]
    canal_id = f"{PREFIJO}UC{clave}"
    uploads_id = f"{PREFIJO}UU{clave}"
    meta.update({
        "videos_extraidos": n,
        "cuota_estimada": 2 + 2 * n_paginas,
        "youtube_channel_id": canal_id,
        "parametros": {"limite": n, "sin_estadisticas": False},
    })

    canal = {
        "kind": "youtube#channelListResponse",
        "pageInfo": {"totalResults": 1, "resultsPerPage": 5},
        "items": [{
            "kind": "youtube#channel",
            "id": canal_id,
            "snippet": {
                "title": f"Canal sintetico {clave}",
                "description": "",
                "customUrl": f"@stress{clave}",
                "publishedAt": _iso(ahora - dt.timedelta(days=400)).replace("+00:00", "Z"),
            },
            "contentDetails": {"relatedPlaylists": {"likes": "", "uploads": uploads_id}},
            "statistics": {"viewCount": str(rnd.randint(0, 10**7)), "subscriberCount": str(rnd.randint(0, 10**5)),
                           "hiddenSubscriberCount": False, "videoCount": str(n)},
        }],
    }

    items_playlist, items_videos = [], []
    for i in range(n):
        video_id = f"{PREFIJO}{clave}-{i:06d}"
        publicado = _iso(ahora - dt.timedelta(seconds=rnd.randint(0, 365 * 86400))).replace("+00:00", "Z")
        titulo = f"Video sintetico {i}"
        descripcion = "Descripcion de prueba " * rnd.randint(1, 10)
        items_playlist.append({
            "kind": "youtube#playlistItem",
            "id": f"{PREFIJO}PI{clave}-{i:06d}",
            "snippet": {
                "publishedAt": publicado, "channelId": canal_id, "title": titulo,
                "description": descripcion, "playlistId": uploads_id, "position": i,
                "resourceId": {"kind": "youtube#video", "videoId": video_id},
            },
            "contentDetails": {"videoId": video_id, "videoPublishedAt": publicado},
            "status": {"privacyStatus": rnd.choice(["public", "public", "public", "unlisted"])},
        })
        items_videos.append({
            "kind": "youtube#video",
            "id": video_id,
            "snippet": {"publishedAt": publicado, "channelId": canal_id, "title": titulo,
                        "description": descripcion},
            "contentDetails": {"duration": f"PT{rnd.randint(0, 59)}M{rnd.randint(1, 59)}S"},
            "statistics": {"viewCount": str(rnd.randint(0, 10**6)), "likeCount": str(rnd.randint(0, 10**4)),
                           "favoriteCount": "0", "commentCount": str(rnd.randint(0, 10**3))},
        })

    paginas_playlist = [
        {"kind": "youtube#playlistItemListResponse",
         "pageInfo": {"totalResults": n, "resultsPerPage": TAMANO_PAGINA},
         "items": items}
        for items in _paginas(items_playlist, n_paginas)
    ]
    estadisticas = [
        {"kind": "youtube#videoListResponse",
         "pageInfo": {"totalResults": len(items), "resultsPerPage": len(items)},
         "items": items}
        for items in _paginas(items_videos, n_paginas)
    ]

    datos = {"metadatos_ejecucion": meta,
             "respuestas": {"canal": canal, "paginas_playlist": paginas_playlist,
                            "estadisticas_videos": estadisticas}}
    return _guardar(datos, carpeta, "youtube", meta["id_ejecucion"])


def _insights(rnd, media_id, media_type, media_product_type):
    metricas = ["reach", "saved"]
    if media_type in ("IMAGE", "VIDEO"):
        metricas += ["likes", "comments", "shares"]
    if media_product_type == "REELS":
        metricas += ["ig_reels_aggregated_all_plays_count"]
    respuesta = {"data": [
        {"name": m, "period": "lifetime", "values": [{"value": rnd.randint(0, 10**4)}],
         "id": f"{media_id}/insights/{m}/lifetime"}
        for m in metricas
    ]}
    return {
        "tipo_publicacion": f"{media_product_type}/{media_type}",
        "metricas_solicitadas": metricas,
        "metricas_descartadas": [],
        "respuesta": respuesta,
        "error": None,
    }


def generar_instagram(n, carpeta, semilla=None):
    """JSON de Instagram Graph API con n publicaciones y sus insights."""
    rnd = random.Random(semilla)
    ahora = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    n_paginas = -(-n // TAMANO_PAGINA)
    meta = _metadatos("instagram", "v21.0", ahora, 1 + n_paginas + n, n_paginas)
    clave = meta["id_ejecucion"][len(PREFIJO):][:8]
    cuenta_id = f"{PREFIJO}IG{clave}"
    meta.update({
        "publicaciones_extraidas": n,
        "ig_business_account_id": cuenta_id,
        "parametros": {"limite": n, "sin_insights": False, "tamano_pagina": TAMANO_PAGINA},
        "metricas_descartadas": [],
    })

    perfil = {"id": cuenta_id, "username": f"stress_{clave}", "name": f"Cuenta sintetica {clave}",
              "followers_count": rnd.randint(0, 10**5), "follows_count": rnd.randint(0, 10**3),
              "media_count": n}

    publicaciones, insights = [], {}
    for i in range(n):
        media_id = f"{PREFIJO}{clave}-{i:06d}"
        media_type, media_product_type = rnd.choice(TIPOS_IG)
        momento = ahora - dt.timedelta(seconds=rnd.randint(0, 365 * 86400))
        publicaciones.append({
            "id": media_id,
            "caption": "Publicacion de prueba #stress " * rnd.randint(1, 8),
            "media_type": media_type,
            "media_product_type": media_product_type,
            "permalink": f"https://www.instagram.com/p/{clave}{i:06d}/",
            "timestamp": momento.strftime("%Y-%m-%dT%H:%M:%S+0000"),
            "like_count": rnd.randint(0, 10**4),
            "comments_count": rnd.randint(0, 10**3),
        })
        insights[media_id] = _insights(rnd, media_id, media_type, media_product_type)

    paginas_media = [
        {"data": items, "paging": {"cursors": {"before": f"c{p}", "after": f"c{p + 1}"}}}
        for p, items in enumerate(_paginas(publicaciones, n_paginas))
    ]

    datos = {"metadatos_ejecucion": meta,
             "respuestas": {"perfil": perfil, "paginas_media": paginas_media, "insights": insights}}
    return _guardar(datos, carpeta, "instagram", meta["id_ejecucion"])
