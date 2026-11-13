import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.dashboard_monitoreo import (
    ZONA, calcular_indicadores, construir_datos, generar_dashboard, leer_integridad,
    leer_metricas_etapas, preparar,
)


def utc(ahora, horas):
    return (ahora - pd.Timedelta(hours=horas)).tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S+00:00")


def datos_prueba(ahora: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame([
        {"id_ejecucion": "a", "plataforma": "youtube", "fecha_hora_inicio_utc": utc(ahora, 50), "duracion_segundos": 10,
         "estado": "EXITO", "contenidos_extraidos": 5, "llamadas_api": 3},
        {"id_ejecucion": "b", "plataforma": "youtube", "fecha_hora_inicio_utc": utc(ahora, 2), "duracion_segundos": 20,
         "estado": "EXITO", "contenidos_extraidos": 6, "llamadas_api": 3},
        {"id_ejecucion": "c", "plataforma": "instagram", "fecha_hora_inicio_utc": utc(ahora, 30), "duracion_segundos": 1,
         "estado": "FALLIDA", "error_tipo": "TokenInvalido", "error_mensaje": "<code>token</code> vencido"},
        {"id_ejecucion": "c", "plataforma": "instagram", "fecha_hora_inicio_utc": utc(ahora, 30), "duracion_segundos": 1,
         "estado": "FALLIDA", "error_tipo": "TokenInvalido"},
        {"id_ejecucion": "d", "plataforma": "youtube", "fecha_hora_inicio_utc": utc(ahora, 24 * 60), "duracion_segundos": 5,
         "estado": "EXITO"},
    ])


def test_preparar_filtra_periodo_y_duplicados():
    ahora = pd.Timestamp.now(tz=ZONA)
    df = preparar(datos_prueba(ahora), 30, ahora)
    assert sorted(df["id_ejecucion"]) == ["a", "b", "c"]


def test_indicadores():
    ahora = pd.Timestamp.now(tz=ZONA)
    ind = calcular_indicadores(preparar(datos_prueba(ahora), 30, ahora), ahora)
    assert ind["total"] == 3
    assert ind["fallos"] == 1
    assert round(ind["tasa_exito"], 1) == 66.7
    assert ind["latencia_prom"] == 15
    assert ind["latencia_max"] == 20
    assert ind["contenidos"] == 11
    assert ind["por_plataforma"]["youtube"]["atrasada"] is False
    assert ind["por_plataforma"]["instagram"]["atrasada"] is True
    assert ind["fallos_por_tipo"]["TokenInvalido"] == 1
    assert ind["fallos_pendientes"] == 1


def test_fallo_resuelto_por_exito_posterior():
    ahora = pd.Timestamp.now(tz=ZONA)
    df = datos_prueba(ahora)
    df.loc[len(df)] = {"id_ejecucion": "e", "plataforma": "instagram", "estado": "EXITO", "duracion_segundos": 2,
                       "fecha_hora_inicio_utc": (ahora - pd.Timedelta(hours=1)).tz_convert("UTC").isoformat()}
    ind = calcular_indicadores(preparar(df, 30, ahora), ahora)
    assert ind["fallos_pendientes"] == 0


def test_alertas():
    ahora = pd.Timestamp.now(tz=ZONA)
    datos = construir_datos(datos_prueba(ahora), pd.DataFrame(), None, "prueba", 30, ahora, dias_token=5)
    titulos = [a["titulo"] for a in datos["alertas"]]
    assert datos["alertas"][0]["nivel"] == "critico"
    assert "Instagram no se pudo actualizar" in titulos
    assert any("Instagram no se actualiza hace" in t for t in titulos)
    assert "El permiso de acceso a Instagram vence en 5 días" in titulos
    assert "Aún no se ha verificado la base de datos" in titulos


def test_resumen_indicadores():
    ahora = pd.Timestamp.now(tz=ZONA)
    datos = construir_datos(datos_prueba(ahora), pd.DataFrame(), None, "prueba", 30, ahora, None)
    assert datos["indicadores"]["total"] == 3
    assert datos["indicadores"]["pendientes"] == 1


def test_metricas_etapas(tmp_path):
    ahora = pd.Timestamp.now(tz=ZONA)
    ruta = tmp_path / "metricas.csv"
    pd.DataFrame([
        {"fecha_inicio_utc": utc(ahora, 1), "plataforma": "todas", "seg_extraccion": 5, "seg_transformacion": 1,
         "seg_carga": 3, "seg_total": 9, "estado_extraccion": "EXITO", "estado_transformacion": "EXITO",
         "estado_carga": "FALLIDA", "registros_originales": 100, "registros_validos": 95},
        {"fecha_inicio_utc": utc(ahora, 24 * 90), "plataforma": "todas", "seg_total": 1},
    ]).to_csv(ruta, index=False)
    etapas = leer_metricas_etapas(30, ahora, ruta)
    assert len(etapas) == 1
    datos = construir_datos(datos_prueba(ahora), etapas, None, "prueba", 30, ahora, None)
    assert datos["etapas"][0]["pct_validos"] == 95.0
    assert any("no terminó bien" in a["titulo"] for a in datos["alertas"])


def test_integridad_guardada(tmp_path):
    ahora = pd.Timestamp.now(tz=ZONA)
    ruta = tmp_path / "integridad.json"
    assert leer_integridad(ruta) is None
    ruta.write_text(json.dumps({"fecha_utc": "2026-01-01T00:00:00+00:00",
                                "pruebas": [{"nombre": "Referencial", "ok": False}], "volumen": {}}), encoding="utf-8")
    integridad = leer_integridad(ruta)
    datos = construir_datos(datos_prueba(ahora), pd.DataFrame(), integridad, "prueba", 30, ahora, None)
    titulos = [a["titulo"] for a in datos["alertas"]]
    assert "Se encontraron diferencias en la base de datos" in titulos
    assert any("no se revisa hace" in t for t in titulos)


def test_genera_html(tmp_path):
    ahora = pd.Timestamp.now(tz=ZONA)
    ruta = generar_dashboard(datos_prueba(ahora), "prueba", 30, tmp_path)
    contenido = ruta.read_text(encoding="utf-8")
    assert "Estado de los datos de redes sociales" in contenido
    assert "__DATOS__" not in contenido
    assert "<code>token</code>" not in contenido.split('id="datos"')[1].split("</script>")[0]


def test_sin_datos(tmp_path):
    ruta = generar_dashboard(pd.DataFrame(), "prueba", 30, tmp_path)
    assert "Estado de los datos de redes sociales" in ruta.read_text(encoding="utf-8")


def test_artefacto_github_y_acumulado(tmp_path):
    import io
    import zipfile
    from tools.dashboard_monitoreo import _acumular, leer_zip_artefacto

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr("data/processed/youtube/ejecucion.csv",
                   "id_ejecucion,plataforma,fecha_hora_inicio_utc,estado,videos_extraidos\nx1,youtube,2026-11-12 10:47:02+00:00,EXITO,5\n")
        z.writestr("data/processed/instagram/ejecucion.csv",
                   "id_ejecucion,plataforma,fecha_hora_inicio_utc,estado,publicaciones_extraidas\nx2,instagram,2026-11-12 10:47:02+00:00,EXITO,12\n")
        z.writestr("logs/metricas_pipeline.csv", "fecha_inicio_utc,plataforma,seg_total\n2026-11-12T10:46:00+00:00,todas,30\n")
    ejecuciones, etapas = leer_zip_artefacto(buffer.getvalue())
    assert len(ejecuciones) == 2 and len(etapas) == 1
    assert "contenidos_extraidos" in ejecuciones[0].columns

    ruta = tmp_path / "ejecuciones.csv"
    _acumular(ejecuciones, ruta, "id_ejecucion")
    df = _acumular(ejecuciones[:1], ruta, "id_ejecucion")
    assert sorted(df["id_ejecucion"]) == ["x1", "x2"]
