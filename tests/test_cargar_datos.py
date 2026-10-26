# -*- coding: utf-8 -*-
"""
Pruebas unitarias para el modulo de carga (src/load/cargar_datos.py)
"""

import datetime as dt
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.load.cargar_datos import (
    generar_filas_tiempo,
    asegurar_fechas_en_dim_tiempo,
    cargar_dim_ejecucion,
    cargar_dim_cuenta,
    cargar_dim_publicacion,
    cargar_hechos_youtube,
    cargar_hechos_instagram,
)


def test_generar_filas_tiempo():
    fechas = {dt.date(2026, 10, 25), dt.date(2026, 1, 1)}
    df = generar_filas_tiempo(fechas)

    assert len(df) == 2
    assert "id_tiempo" in df.columns
    assert "nombre_mes" in df.columns
    assert "es_fin_de_semana" in df.columns

    # Verificar 2026-10-25 (Domingo)
    fila_oct = df[df["id_tiempo"] == 20261025].iloc[0]
    assert fila_oct["anio"] == 2026
    assert fila_oct["mes"] == 10
    assert fila_oct["nombre_mes"] == "Octubre"
    assert fila_oct["nombre_dia"] == "Domingo"
    assert fila_oct["trimestre"] == 4
    assert fila_oct["es_fin_de_semana"] == 1

    # Verificar 2026-01-01 (Jueves)
    fila_ene = df[df["id_tiempo"] == 20260101].iloc[0]
    assert fila_ene["nombre_mes"] == "Enero"
    assert fila_ene["trimestre"] == 1
    assert fila_ene["es_fin_de_semana"] == 0


def test_asegurar_fechas_en_dim_tiempo_vacio():
    mock_engine = MagicMock()
    resultado = asegurar_fechas_en_dim_tiempo(mock_engine, set())
    assert resultado == 0


def test_cargar_dim_ejecucion_vacio():
    mock_engine = MagicMock()
    assert cargar_dim_ejecucion(mock_engine, None) == 0
    assert cargar_dim_ejecucion(mock_engine, pd.DataFrame()) == 0


def test_cargar_dim_cuenta_mapa_existente():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = [
        ("youtube", "UC123", 1),
        ("instagram", "IG456", 2),
    ]

    # DataFrame con cuentas que ya existen
    df_cuenta = pd.DataFrame([
        {"plataforma": "youtube", "id_cuenta": "UC123", "usuario": "yt_user", "nombre": "YT"},
        {"plataforma": "instagram", "id_cuenta": "IG456", "usuario": "ig_user", "nombre": "IG"},
    ])

    insertadas, mapa = cargar_dim_cuenta(mock_engine, df_cuenta)
    assert insertadas == 0
    assert mapa[("youtube", "UC123")] == 1
    assert mapa[("instagram", "IG456")] == 2


def test_cargar_dim_publicacion_mapeo():
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = [
        ("youtube", "VID_EXISTENTE", 10)
    ]

    mapa_cuentas = {("youtube", "UC123"): 1}
    df_contenidos = pd.DataFrame([
        {
            "plataforma": "youtube",
            "id_contenido": "VID_EXISTENTE",
            "id_cuenta": "UC123",
            "titulo": "Video 1",
        }
    ])

    insertadas, mapa_pub = cargar_dim_publicacion(mock_engine, df_contenidos, mapa_cuentas)
    assert insertadas == 0
    assert mapa_pub[("youtube", "VID_EXISTENTE")] == 10
