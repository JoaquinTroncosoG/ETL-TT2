#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pruebas de src/transform/cargar_json.py

No dependen de data/raw (esta en el .gitignore): cada prueba arma sus propios
JSON en una carpeta temporal, con la misma estructura que escribe
extract_instagram.py. El JSON "con publicaciones" es SINTETICO: la cuenta real
aun no tiene contenido.

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
