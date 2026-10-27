#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas de src/transform/cargar_json.py

No dependen de data/raw (esta en el .gitignore): cada prueba arma sus propios
JSON en una carpeta temporal, con la misma estructura que escribe
extract_instagram.py y extract_youtube.py. Los JSON con contenido son
SINTETICOS: la cuenta de Instagram aun no tiene publicaciones y todavia no hay
extracciones reales de YouTube.

USO
    python -m unittest tests.prueba_cargar_json -v
"""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transform import cargar_json as cj  # noqa: E402


def _meta(id_ejecucion, estado="EXITO", error=None):
    return {
        "id_ejecucion": id_ejecucion, "plataforma": "instagram",
        "version_api": "v21.0",
        "fecha_hora_inicio_utc": "2026-10-01T00:00:00+00:00",
        "fecha_hora_fin_utc": "2026-10-01T00:00:02+00:00",
        "duracion_segundos": 1.5, "estado": estado,
        "publicaciones_extraidas": 0, "llamadas_api": 2, "paginas_recorridas": 1,
        "error": error, "ig_business_account_id": "17841422722303323",
        "parametros": {"limite": None, "sin_insights": False},
        "metricas_descartadas": [],
        "x_business_use_case_usage": [{"llamada": 1, "valor": {"x": []}}],
    }


JSON_CUENTA_VACIA = {
    "metadatos_ejecucion": _meta("aaaa-1111"),
    "respuestas": {
        "perfil": {"id": "1784", "username": "leoricdeastora", "name": "Joaquin Troncoso",
                   "followers_count": 0, "follows_count": 0, "media_count": 0},
        "paginas_media": [{"data": []}],
        "insights": None,
    },
}

JSON_FALLIDA = {
    "metadatos_ejecucion": _meta("bbbb-2222", "FALLIDA", {
        "tipo": "ErrorApi", "mensaje": "missing permissions", "estado_http": 400,
        "codigo": 100, "subcodigo": 33, "fbtrace_id": "ACR123"}),
    "respuestas": {"perfil": None, "paginas_media": [], "insights": {}},
}

JSON_CON_PUBLICACIONES = {
    "metadatos_ejecucion": _meta("cccc-3333"),
    "respuestas": {
        "perfil": {"id": "1784", "username": "cuenta", "name": "Cuenta",
                   "followers_count": 12, "follows_count": 3, "media_count": 3},
        "paginas_media": [
            {"data": [
                {"id": "m1", "caption": "foto", "media_type": "IMAGE",
                 "media_product_type": "FEED", "permalink": "https://x/1",
                 "timestamp": "2026-09-30T12:00:00+0000", "like_count": 5,
                 "comments_count": 0, "campo_extra_ruido": "no debe pasar"},
                {"id": "m2", "caption": "reel", "media_type": "VIDEO",
                 "media_product_type": "REELS", "permalink": "https://x/2",
                 "timestamp": "2026-09-29T12:00:00+0000", "like_count": 0,
                 "comments_count": 1},
            ]},
            {"data": [
                {"id": "m3", "media_type": "IMAGE", "media_product_type": "FEED",
                 "permalink": "https://x/3", "timestamp": "2026-09-28T12:00:00+0000",
                 "like_count": 1, "comments_count": 0},
            ]},
        ],
        "insights": {
            "m1": {"tipo_publicacion": "FEED/IMAGE", "error": None, "respuesta": {"data": [
                {"name": "reach", "period": "lifetime", "values": [{"value": 40}],
                 "title": "Reach", "description": "ruido", "id": "m1/insights/reach/lifetime"},
                {"name": "saved", "period": "lifetime", "values": [{"value": 0}]},
            ]}},
            "m2": {"tipo_publicacion": "REELS/VIDEO", "respuesta": None,
                   "error": {"tipo": "ErrorApi", "mensaje": "sin insights"}},
            # m3 sin registro de insights
        },
    },
}


def _meta_yt(id_ejecucion, estado="EXITO", error=None):
    return {
        "id_ejecucion": id_ejecucion, "plataforma": "youtube", "version_api": "v3",
        "fecha_hora_inicio_utc": "2026-10-06T00:00:00+00:00",
        "fecha_hora_fin_utc": "2026-10-06T00:00:03+00:00",
        "duracion_segundos": 2.1, "estado": estado, "videos_extraidos": 3,
        "llamadas_api": 4, "paginas_recorridas": 2, "cuota_estimada": 4,
        "error": error, "youtube_channel_id": "UC123",
        "parametros": {"limite": None, "sin_estadisticas": False},
    }


def _item_playlist(video_id, titulo):
    return {"kind": "youtube#playlistItem", "etag": "ruido",
            "snippet": {"title": titulo, "description": "desc", "thumbnails": {"default": {}}},
            "contentDetails": {"videoId": video_id,
                               "videoPublishedAt": "2026-09-30T12:00:00Z"},
            "status": {"privacyStatus": "public"}}


def _item_video(video_id, vistas):
    return {"kind": "youtube#video", "etag": "ruido", "id": video_id,
            "contentDetails": {"duration": "PT4M13S", "definition": "hd"},
            "statistics": {"viewCount": vistas, "likeCount": "0", "commentCount": "2",
                           "favoriteCount": "0"},
            "topicDetails": {"topicCategories": ["https://es.wikipedia.org/wiki/Music"]}}


JSON_YT_CON_VIDEOS = {
    "metadatos_ejecucion": _meta_yt("dddd-4444"),
    "respuestas": {
        "canal": {"items": [{
            "id": "UC123",
            "snippet": {"title": "Canal", "customUrl": "@canal",
                        "publishedAt": "2020-01-01T00:00:00Z"},
            "statistics": {"subscriberCount": "0", "videoCount": "3", "viewCount": "150",
                           "hiddenSubscriberCount": False},
            "contentDetails": {"relatedPlaylists": {"uploads": "UU123"}},
        }]},
        "paginas_playlist": [
            {"items": [_item_playlist("v1", "uno"), _item_playlist("v2", "dos")],
             "nextPageToken": "abc"},
            {"items": [_item_playlist("v3", "tres")]},
        ],
        "estadisticas_videos": [
            {"items": [_item_video("v1", "100"), _item_video("v2", "50"),
                       _item_video("v3", "0")]},
        ],
    },
}

JSON_YT_SIN_ESTADISTICAS = {
    "metadatos_ejecucion": _meta_yt("eeee-5555"),
    "respuestas": {
        "canal": JSON_YT_CON_VIDEOS["respuestas"]["canal"],
        "paginas_playlist": [{"items": [_item_playlist("v1", "uno")]}],
        "estadisticas_videos": None,
    },
}

JSON_YT_FALLIDA = {
    "metadatos_ejecucion": _meta_yt("ffff-6666", "FALLIDA", {
        "tipo": "ErrorApi", "mensaje": "The request cannot be completed",
        "estado_http": 403, "codigo": None, "razon": "forbidden"}),
    "respuestas": {"canal": None, "paginas_playlist": [], "estadisticas_videos": []},
}


class PruebaCargarJson(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _escribir(self, nombre, contenido):
        ruta = self.dir / nombre
        ruta.write_text(json.dumps(contenido), encoding="utf-8")
        return ruta

    def test_cuenta_vacia_conserva_los_ceros(self):
        t = cj.cargar_instagram(self._escribir("instagram_a.json", JSON_CUENTA_VACIA))
        self.assertEqual(len(t["perfil"]), 1)
        fila = t["perfil"].iloc[0]
        self.assertEqual(fila["followers_count"], 0)
        self.assertEqual(fila["media_count"], 0)
        self.assertEqual(fila["id_ejecucion"], "aaaa-1111")
        self.assertTrue(t["publicaciones"].empty)
        self.assertTrue(t["metricas"].empty)

    def test_ejecucion_fallida_registra_el_error_y_no_inventa_perfil(self):
        t = cj.cargar_instagram(self._escribir("instagram_b.json", JSON_FALLIDA))
        self.assertTrue(t["perfil"].empty)
        ej = t["ejecucion"].iloc[0]
        self.assertEqual(ej["estado"], "FALLIDA")
        self.assertEqual(ej["error_codigo"], 100)
        self.assertIn("missing permissions", ej["error_mensaje"])

    def test_ejecucion_exitosa_deja_campos_de_error_nulos(self):
        t = cj.cargar_instagram(self._escribir("instagram_a.json", JSON_CUENTA_VACIA))
        ej = t["ejecucion"].iloc[0]
        self.assertTrue(ej[["error_tipo", "error_codigo", "error_mensaje"]].isna().all())

    def test_publicaciones_de_varias_paginas_se_juntan(self):
        t = cj.cargar_instagram(self._escribir("instagram_c.json", JSON_CON_PUBLICACIONES))
        self.assertEqual(list(t["publicaciones"]["id"]), ["m1", "m2", "m3"])

    def test_solo_quedan_las_columnas_de_la_lista_blanca(self):
        t = cj.cargar_instagram(self._escribir("instagram_c.json", JSON_CON_PUBLICACIONES))
        self.assertEqual(list(t["publicaciones"].columns), cj.COLUMNAS_PUBLICACION)
        self.assertNotIn("campo_extra_ruido", t["publicaciones"].columns)
        self.assertEqual(list(t["ejecucion"].columns), cj.COLUMNAS_EJECUCION)
        self.assertNotIn("x_business_use_case_usage", t["ejecucion"].columns)
        self.assertNotIn("fbtrace_id", t["ejecucion"].columns)

    def test_metricas_en_formato_largo_y_sin_ruido(self):
        t = cj.cargar_instagram(self._escribir("instagram_c.json", JSON_CON_PUBLICACIONES))
        m = t["metricas"]
        self.assertEqual(list(m.columns), cj.COLUMNAS_METRICA)
        self.assertEqual(len(m), 2)  # solo m1; m2 con error y m3 sin registro no generan filas
        self.assertEqual(set(m["id_publicacion"]), {"m1"})
        self.assertEqual(int(m.loc[m["metrica"] == "saved", "valor"].iloc[0]), 0)

    def test_cargar_todo_concatena_y_mantiene_el_id_de_ejecucion(self):
        self._escribir("instagram_a.json", JSON_CUENTA_VACIA)
        self._escribir("instagram_b.json", JSON_FALLIDA)
        self._escribir("instagram_c.json", JSON_CON_PUBLICACIONES)
        t = cj.cargar_todo_instagram(self.dir)
        self.assertEqual(len(t["ejecucion"]), 3)
        self.assertEqual(len(t["perfil"]), 2)          # la fallida no aporta perfil
        self.assertEqual(len(t["publicaciones"]), 3)
        self.assertEqual(set(t["perfil"]["id_ejecucion"]), {"aaaa-1111", "cccc-3333"})

    def test_carpeta_sin_archivos_devuelve_tablas_vacias_con_columnas(self):
        t = cj.cargar_todo_instagram(self.dir)
        self.assertEqual(set(t), {"ejecucion", "perfil", "publicaciones", "metricas"})
        self.assertTrue(all(df.empty for df in t.values()))
        self.assertEqual(list(t["perfil"].columns), cj.COLUMNAS_PERFIL)

    # ---------------------------------------------------------------- YouTube

    def test_youtube_canal_y_videos_de_varias_paginas(self):
        t = cj.cargar_youtube(self._escribir("youtube_d.json", JSON_YT_CON_VIDEOS))
        self.assertEqual(len(t["canal"]), 1)
        self.assertEqual(t["canal"].iloc[0]["statistics.subscriberCount"], "0")
        self.assertEqual(list(t["videos"]["contentDetails.videoId"]), ["v1", "v2", "v3"])
        self.assertEqual(list(t["estadisticas"]["id"]), ["v1", "v2", "v3"])
        self.assertEqual(set(t["estadisticas"]["id_ejecucion"]), {"dddd-4444"})

    def test_youtube_solo_quedan_las_columnas_de_la_lista_blanca(self):
        t = cj.cargar_youtube(self._escribir("youtube_d.json", JSON_YT_CON_VIDEOS))
        self.assertEqual(list(t["ejecucion"].columns), cj.COLUMNAS_EJECUCION_YT)
        self.assertEqual(list(t["canal"].columns), cj.COLUMNAS_CANAL)
        self.assertEqual(list(t["videos"].columns), cj.COLUMNAS_VIDEO)
        self.assertEqual(list(t["estadisticas"].columns), cj.COLUMNAS_ESTADISTICA)

    def test_youtube_sin_estadisticas_conserva_el_listado(self):
        t = cj.cargar_youtube(self._escribir("youtube_e.json", JSON_YT_SIN_ESTADISTICAS))
        self.assertEqual(len(t["videos"]), 1)
        self.assertTrue(t["estadisticas"].empty)

    def test_youtube_fallida_usa_el_estado_http_como_codigo(self):
        t = cj.cargar_youtube(self._escribir("youtube_f.json", JSON_YT_FALLIDA))
        ej = t["ejecucion"].iloc[0]
        self.assertEqual(ej["estado"], "FALLIDA")
        self.assertEqual(ej["error_codigo"], 403)
        self.assertTrue(t["canal"].empty)
        self.assertTrue(t["videos"].empty)

    def test_youtube_cargar_todo_ignora_los_json_de_instagram(self):
        self._escribir("youtube_d.json", JSON_YT_CON_VIDEOS)
        self._escribir("youtube_f.json", JSON_YT_FALLIDA)
        self._escribir("instagram_a.json", JSON_CUENTA_VACIA)
        t = cj.cargar_todo_youtube(self.dir)
        self.assertEqual(len(t["ejecucion"]), 2)
        self.assertEqual(len(t["canal"]), 1)
        self.assertEqual(len(t["videos"]), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
