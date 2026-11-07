# -*- coding: utf-8 -*-
"""
=============================================================================
PRUEBAS DE INTEGRIDAD: DATA CRUDA (RAW) VS AZURE SQL DATABASE
=============================================================================
Pruebas automatizadas con pytest para verificar la consistencia,
reconciliacion de datos y fidelidad del Data Warehouse en Azure SQL.
=============================================================================
"""

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from src.load.cargar_datos import obtener_engine
from tools.verificar_integridad import (
    cargar_todos_los_raw,
    auditar_conciliacion_volumen,
    auditar_fidelidad_atributos_publicacion,
    auditar_exactitud_metricas,
    auditar_integridad_referencial,
    auditar_unicidad_y_deduplicacion,
    auditar_consistencia_temporal,
)


@pytest.fixture(scope="module")
def db_engine():
    """
    Fixture que provee la conexion a Azure SQL Database.
    Si la base de datos no esta accesible (offline/sin red), salta los tests.
    """
    try:
        engine = obtener_engine()
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        return engine
    except Exception as e:
        pytest.skip(f"Azure SQL Database no accesible para pruebas de integracion: {e}")


@pytest.fixture(scope="module")
def raw_data():
    """Fixture con todos los datos crudos parseados desde data/raw/."""
    return cargar_todos_los_raw()


# =============================================================================
# TESTS
# =============================================================================

def test_conciliacion_volumen(db_engine: Engine, raw_data):
    """Verifica que las entidades crudas esten presentes en su totalidad en el DW."""
    ok, resumen = auditar_conciliacion_volumen(db_engine, raw_data)
    assert ok is True, f"Fallo en la conciliacion de volumen: {resumen}"
    assert resumen["cuentas_dw"] >= resumen["cuentas_raw"]
    assert resumen["pubs_dw"] >= resumen["pubs_raw"]
    assert resumen["ejec_dw"] >= resumen["ejec_raw"]


def test_fidelidad_atributos_publicaciones(db_engine: Engine, raw_data):
    """Verifica la fidelidad campo a campo (tipos, permalink, UTC) en DIM_PUBLICACION."""
    ok = auditar_fidelidad_atributos_publicacion(db_engine, raw_data)
    assert ok is True, "Discrepancias detectadas en atributos de DIM_PUBLICACION"


def test_exactitud_metricas_cuantitativas(db_engine: Engine, raw_data):
    """Verifica que los valores de likes, comentarios, alcance coincidan con el raw."""
    ok = auditar_exactitud_metricas(db_engine, raw_data)
    assert ok is True, "Discrepancias cuantitativas en HECHOS_METRICAS_PUBLICACION"


def test_integridad_referencial_cero_huerfanos(db_engine: Engine):
    """Verifica que no existan claves foraneas huerfanas en el modelo en estrella."""
    ok = auditar_integridad_referencial(db_engine)
    assert ok is True, "Se encontraron registros huerfanos en las relaciones dimensionales"


def test_unicidad_y_deduplicacion(db_engine: Engine):
    """Verifica que no existan registros duplicados en claves primarias o de negocio."""
    ok = auditar_unicidad_y_deduplicacion(db_engine)
    assert ok is True, "Se detectaron tuplas duplicadas en las tablas del DW"


def test_consistencia_temporal_utc(db_engine: Engine):
    """Verifica la correspondencia entre id_tiempo y timestamps UTC."""
    ok = auditar_consistencia_temporal(db_engine)
    assert ok is True, "Inconsistencias temporales detectadas entre hechos y dimensiones"
