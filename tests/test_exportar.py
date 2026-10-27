import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transform.transform_data import exportar_resultados  # noqa: E402

FECHAS = ["fecha_hora_inicio_utc", "fecha_hora_fin_utc"]
TIPO_UTC = "datetime64[ns, UTC]"


@pytest.fixture
def ejecucion():
    df = pd.DataFrame({
        "id_ejecucion": ["exec-001", "exec-002"],
        "plataforma": ["youtube", "youtube"],
        "version_api": ["v3", "v3"],
        "fecha_hora_inicio_utc": ["2026-10-20T12:00:00+00:00", "2026-10-21T15:30:00+00:00"],
        "fecha_hora_fin_utc": ["2026-10-20T12:01:30+00:00", "2026-10-21T15:30:45+00:00"],
        "duracion_segundos": [90.5, 45.0],
        "estado": ["exito", "error"],
        "videos_extraidos": [50, 0],
        "llamadas_api": [3, 1],
        "paginas_recorridas": [2, 0],
        "cuota_estimada": [5, 1],
        "youtube_channel_id": ["UC123abc", "UC123abc"],
        "error_tipo": ["ninguno", "HttpError"],
        "error_codigo": ["ninguno", "quotaExceeded"],
        "error_mensaje": ["ninguno", "Cuota diaria, agotada"],
    })
    for col in FECHAS:
        df[col] = pd.to_datetime(df[col], utc=True).dt.as_unit("ns")
    return df


def releer_csv(ruta, **kwargs):
    df = pd.read_csv(ruta, **kwargs)
    for col in FECHAS:
        df[col] = pd.to_datetime(df[col], utc=True).dt.as_unit("ns")
    return df


def test_csv_principal_sin_sep_y_mismo_contenido(ejecucion, tmp_path):
    exportar_resultados({"ejecucion": ejecucion}, tmp_path, "youtube")
    ruta = tmp_path / "youtube" / "ejecucion.csv"

    primera = ruta.read_text(encoding="utf-8-sig").splitlines()[0]
    assert "sep=," not in primera
    assert ",fecha_hora_inicio_utc," in primera
    assert "+00:00" in ruta.read_text(encoding="utf-8-sig")

    df = releer_csv(ruta)
    assert list(df.columns) == list(ejecucion.columns)
    for col in FECHAS:
        assert str(df[col].dtype) == TIPO_UTC
    for col in ejecucion.columns:
        assert df[col].dtype.kind == ejecucion[col].dtype.kind, col
    pd.testing.assert_frame_equal(df, ejecucion, check_dtype=False)
    pd.testing.assert_frame_equal(df[FECHAS], ejecucion[FECHAS])


def test_csv_excel_con_sep_y_mismo_contenido(ejecucion, tmp_path):
    exportar_resultados({"ejecucion": ejecucion}, tmp_path, "youtube")
    carpeta = tmp_path / "youtube"
    ruta = carpeta / "ejecucion_excel.csv"

    assert ruta.read_text(encoding="utf-8-sig").splitlines()[0] == "sep=,"

    principal = releer_csv(carpeta / "ejecucion.csv")
    excel = releer_csv(ruta, skiprows=1)
    pd.testing.assert_frame_equal(excel, principal)


def test_xlsx_mismo_contenido(ejecucion, tmp_path):
    exportar_resultados({"ejecucion": ejecucion}, tmp_path, "youtube")
    df = pd.read_excel(tmp_path / "youtube" / "ejecucion.xlsx")

    for col in FECHAS:
        assert df[col].dt.tz is None
        df[col] = df[col].dt.tz_localize("UTC").dt.as_unit("ns")
    pd.testing.assert_frame_equal(df, ejecucion, check_dtype=False)


def test_solo_csv_no_genera_otros(ejecucion, tmp_path):
    archivos = exportar_resultados({"ejecucion": ejecucion}, tmp_path, "youtube",
                                   formatos=("csv",))
    carpeta = tmp_path / "youtube"

    assert archivos == [carpeta / "ejecucion.csv"]
    assert sorted(p.name for p in carpeta.iterdir()) == ["ejecucion.csv"]


def test_formato_invalido(ejecucion, tmp_path):
    with pytest.raises(ValueError):
        exportar_resultados({"ejecucion": ejecucion}, tmp_path, "youtube", formatos=("json",))
