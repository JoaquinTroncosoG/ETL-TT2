import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transform import cargar_json as cj  # noqa: E402
from src.transform.normalizar import (  # noqa: E402
    COLUMNAS, TIPO_FECHA, a_entero, a_snake_case, a_utc, duracion_a_segundos,
    normalizar_instagram, normalizar_youtube,
)


def utc(texto):
    return pd.Timestamp(texto, tz="UTC")


@pytest.mark.parametrize("entrada, esperado", [
    ("2026-09-30T12:00:00Z", "2026-09-30 12:00:00"),
    ("2026-09-30T12:00:00+0000", "2026-09-30 12:00:00"),
    ("2026-09-30T12:00:00+00:00", "2026-09-30 12:00:00"),
    ("2026-09-30T09:00:00-03:00", "2026-09-30 12:00:00"),
    ("2026-09-30T12:00:00.123Z", "2026-09-30 12:00:00.123"),
    ("  2026-09-30T12:00:00Z  ", "2026-09-30 12:00:00"),
    # sin zona: America/Santiago, invierno UTC-4 y verano UTC-3
    ("2026-07-01T10:00:00", "2026-07-01 14:00:00"),
    ("2026-01-15T10:00:00", "2026-01-15 13:00:00"),
    ("2026-07-01", "2026-07-01 04:00:00"),
])
def test_a_utc_convierte(entrada, esperado):
    assert a_utc(pd.Series([entrada]))[0] == utc(esperado)


def test_a_utc_hora_inexistente_por_cambio_de_horario():
    # 2026-09-06 a las 00:00 Chile adelanta a 01:00
    assert a_utc(pd.Series(["2026-09-06T00:30:00"]))[0] == utc("2026-09-06 04:00:00")


def test_a_utc_hora_ambigua_por_cambio_de_horario():
    # 2026-04-04 a las 24:00 Chile vuelve a 23:00; se toma la primera (UTC-3)
    assert a_utc(pd.Series(["2026-04-04T23:30:00"]))[0] == utc("2026-04-05 02:30:00")


@pytest.mark.parametrize("entrada", [None, "", "   ", "no es fecha", "2026-13-45", 12345, float("nan")])
def test_a_utc_invalidos_quedan_nat(entrada):
    assert pd.isna(a_utc(pd.Series([entrada], dtype=object))[0])


def test_a_utc_formatos_mezclados_y_tipo():
    serie = a_utc(pd.Series(["2026-09-30T12:00:00Z", "2026-09-30T12:00:00+0000",
                             "2026-07-01T10:00:00", None]))
    assert str(serie.dtype) == TIPO_FECHA
    assert list(serie[:3]) == [utc("2026-09-30 12:00"), utc("2026-09-30 12:00"),
                               utc("2026-07-01 14:00")]
    assert pd.isna(serie[3])


def test_a_utc_es_idempotente():
    una = a_utc(pd.Series(["2026-07-01T10:00:00", "2026-09-30T12:00:00Z"]))
    pd.testing.assert_series_equal(a_utc(una), una)


def test_a_utc_serie_vacia():
    assert str(a_utc(pd.Series([], dtype=object)).dtype) == TIPO_FECHA


def test_a_entero():
    serie = a_entero(pd.Series(["100", 5, " 7 ", "12.0", -3, None, "", "abc", "12.5", True]))
    assert str(serie.dtype) == "Int64"
    assert serie.tolist()[:5] == [100, 5, 7, 12, -3]
    assert serie[5:].isna().all()


def test_a_entero_numero_grande_y_idempotente():
    serie = a_entero(pd.Series(["9007199254740991"]))
    assert serie[0] == 9007199254740991
    pd.testing.assert_series_equal(a_entero(serie), serie)


@pytest.mark.parametrize("entrada, esperado", [
    ("PT4M13S", 253), ("PT1H2M3S", 3723), ("PT30S", 30), ("PT2H", 7200),
    ("P1DT1S", 86401), ("P0D", 0),
])
def test_duracion_a_segundos(entrada, esperado):
    assert duracion_a_segundos(pd.Series([entrada]))[0] == esperado


@pytest.mark.parametrize("entrada", ["", "PT", "4M13S", "abc", None])
def test_duracion_invalida_queda_na(entrada):
    assert pd.isna(duracion_a_segundos(pd.Series([entrada], dtype=object))[0])


@pytest.mark.parametrize("entrada, esperado", [
    ("statistics.viewCount", "statistics_view_count"),
    ("snippet.customUrl", "snippet_custom_url"),
    ("Like Count", "like_count"),
    ("ya_snake", "ya_snake"),
])
def test_a_snake_case(entrada, esperado):
    assert a_snake_case(entrada) == esperado


# --- tablas completas ---

def tablas_instagram():
    return {
        "ejecucion": pd.DataFrame([{
            "id_ejecucion": "e1", "plataforma": "instagram", "version_api": "v21.0",
            "fecha_hora_inicio_utc": "2026-10-01T00:00:00+00:00",
            "fecha_hora_fin_utc": None, "duracion_segundos": 1.5, "estado": "EXITO",
            "publicaciones_extraidas": 2, "llamadas_api": 3, "paginas_recorridas": 1,
            "ig_business_account_id": "1784", "error_tipo": None, "error_codigo": None,
            "error_mensaje": None}], columns=cj.COLUMNAS_EJECUCION),
        "perfil": pd.DataFrame([{
            "id_ejecucion": "e1", "id": "1784", "username": "cuenta", "name": "Cuenta",
            "followers_count": 12, "follows_count": None, "media_count": 2}],
            columns=cj.COLUMNAS_PERFIL),
        "publicaciones": pd.DataFrame([
            {"id_ejecucion": "e1", "id": "m1", "caption": "foto", "media_type": "IMAGE",
             "media_product_type": "FEED", "permalink": "https://x/1",
             "timestamp": "2026-09-30T12:00:00+0000", "like_count": 5, "comments_count": None},
            {"id_ejecucion": "e1", "id": "m2", "caption": None, "media_type": "VIDEO",
             "media_product_type": "REELS", "permalink": "https://x/2",
             "timestamp": "2026-07-01T10:00:00", "like_count": "0", "comments_count": 1},
        ], columns=cj.COLUMNAS_PUBLICACION),
        "metricas": pd.DataFrame([
            {"id_ejecucion": "e1", "id_publicacion": "m1", "metrica": "reach",
             "periodo": "lifetime", "valor": 40}], columns=cj.COLUMNAS_METRICA),
    }


def tablas_youtube():
    return {
        "ejecucion": pd.DataFrame([{
            "id_ejecucion": "e2", "plataforma": "youtube", "version_api": "v3",
            "fecha_hora_inicio_utc": "2026-10-06T00:00:00+00:00",
            "fecha_hora_fin_utc": "2026-10-06T00:00:03+00:00", "duracion_segundos": 2.1,
            "estado": "FALLIDA", "videos_extraidos": 1, "llamadas_api": 4,
            "paginas_recorridas": 2, "cuota_estimada": 4, "youtube_channel_id": "UC1",
            "error_tipo": "ErrorApi", "error_codigo": 403, "error_mensaje": "quota"}],
            columns=cj.COLUMNAS_EJECUCION_YT),
        "canal": pd.DataFrame([{
            "id_ejecucion": "e2", "id": "UC1", "snippet.title": "Canal",
            "snippet.customUrl": "@canal", "snippet.publishedAt": "2020-01-01T00:00:00Z",
            "statistics.subscriberCount": "10", "statistics.videoCount": "1",
            "statistics.viewCount": "150"}], columns=cj.COLUMNAS_CANAL),
        "videos": pd.DataFrame([{
            "id_ejecucion": "e2", "contentDetails.videoId": "v1", "snippet.title": "uno",
            "snippet.description": "desc", "contentDetails.videoPublishedAt": "2026-09-30T12:00:00Z",
            "status.privacyStatus": "public"}], columns=cj.COLUMNAS_VIDEO),
        "estadisticas": pd.DataFrame([{
            "id_ejecucion": "e2", "id": "v1", "contentDetails.duration": "PT4M13S",
            "statistics.viewCount": "100", "statistics.likeCount": None,
            "statistics.commentCount": "2"}], columns=cj.COLUMNAS_ESTADISTICA),
    }


def test_normalizar_instagram():
    t = normalizar_instagram(tablas_instagram())
    assert set(t) == {"ejecucion", "cuenta", "contenidos", "metricas"}

    c = t["contenidos"]
    assert c["fecha_hora_publicacion_utc"].tolist() == [utc("2026-09-30 12:00"),
                                                        utc("2026-07-01 14:00")]
    assert c["cantidad_me_gusta"].tolist() == [5, 0]
    assert pd.isna(c["cantidad_comentarios"][0])
    assert pd.isna(c["texto"][1])
    assert (c["plataforma"] == "instagram").all()
    assert t["cuenta"]["id_cuenta"][0] == "1784"
    assert pd.isna(t["cuenta"]["cantidad_seguidos"][0])
    assert t["metricas"]["id_contenido"][0] == "m1"
    assert pd.isna(t["ejecucion"]["fecha_hora_fin_utc"][0])


def test_normalizar_youtube():
    t = normalizar_youtube(tablas_youtube())
    assert set(t) == {"ejecucion", "cuenta", "contenidos", "estadisticas"}
    assert t["cuenta"]["cantidad_vistas"][0] == 150
    assert t["cuenta"]["fecha_hora_creacion_utc"][0] == utc("2020-01-01")
    assert t["contenidos"]["id_contenido"][0] == "v1"
    e = t["estadisticas"]
    assert e["duracion_video_segundos"][0] == 253
    assert e["cantidad_vistas"][0] == 100
    assert pd.isna(e["cantidad_me_gusta"][0])
    assert t["ejecucion"]["error_codigo"][0] == 403
    assert t["ejecucion"]["contenidos_extraidos"][0] == 1


@pytest.mark.parametrize("tabla", ["ejecucion", "cuenta", "contenidos"])
def test_esquema_comun_entre_plataformas(tabla):
    ig = normalizar_instagram(tablas_instagram())[tabla]
    yt = normalizar_youtube(tablas_youtube())[tabla]
    assert list(ig.columns) == list(yt.columns) == COLUMNAS[tabla]
    pd.testing.assert_series_equal(ig.dtypes, yt.dtypes)
    assert len(pd.concat([ig, yt], ignore_index=True)) == len(ig) + len(yt)


def test_tipos_del_esquema():
    t = normalizar_youtube(tablas_youtube())
    for df in t.values():
        for columna, tipo in df.dtypes.items():
            if columna.startswith("fecha_"):
                assert str(tipo) == TIPO_FECHA
            elif columna.startswith("cantidad_") or columna == "valor":
                assert str(tipo) == "Int64"


def test_tablas_vacias_conservan_esquema():
    vacias = {n: pd.DataFrame(columns=df.columns) for n, df in tablas_youtube().items()}
    t = normalizar_youtube(vacias)
    llenas = normalizar_youtube(tablas_youtube())
    for nombre, df in t.items():
        assert df.empty
        pd.testing.assert_series_equal(df.dtypes, llenas[nombre].dtypes)


def test_columnas_faltantes_y_extras():
    tablas = tablas_instagram()
    tablas["publicaciones"] = tablas["publicaciones"].drop(columns=["like_count"])
    tablas["publicaciones"]["campoExtra.subCampo"] = "x"
    c = normalizar_instagram(tablas)["contenidos"]
    assert c["cantidad_me_gusta"].isna().all()
    assert str(c["cantidad_me_gusta"].dtype) == "Int64"
    assert c.columns[-1] == "campo_extra_sub_campo"


def test_tabla_ausente_no_rompe():
    tablas = tablas_youtube()
    del tablas["estadisticas"]
    assert "estadisticas" not in normalizar_youtube(tablas)


def test_no_modifica_la_entrada():
    tablas = tablas_instagram()
    original = tablas["publicaciones"].copy()
    normalizar_instagram(tablas)
    pd.testing.assert_frame_equal(tablas["publicaciones"], original)


def test_normalizar_es_idempotente():
    una = normalizar_instagram(tablas_instagram())
    dos = normalizar_instagram(una | {"perfil": una["cuenta"], "publicaciones": una["contenidos"]})
    for nombre in ("ejecucion", "cuenta", "contenidos", "metricas"):
        pd.testing.assert_frame_equal(dos[nombre], una[nombre])


def test_desde_json_crudo(tmp_path):
    from tests.prueba_cargar_json import JSON_CON_PUBLICACIONES, JSON_YT_CON_VIDEOS
    (tmp_path / "ig.json").write_text(json.dumps(JSON_CON_PUBLICACIONES), encoding="utf-8")
    (tmp_path / "yt.json").write_text(json.dumps(JSON_YT_CON_VIDEOS), encoding="utf-8")
    ig = normalizar_instagram(cj.cargar_instagram(tmp_path / "ig.json"))
    yt = normalizar_youtube(cj.cargar_youtube(tmp_path / "yt.json"))
    assert ig["contenidos"]["fecha_hora_publicacion_utc"][0] == utc("2026-09-30 12:00")
    assert yt["estadisticas"]["cantidad_vistas"].tolist() == [100, 50, 0]
    assert len(ig["metricas"]) == 2
