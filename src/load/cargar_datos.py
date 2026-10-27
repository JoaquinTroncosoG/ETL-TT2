#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Carga de datos transformados a Azure SQL Database (Data Warehouse)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Fase 3 (Carga): Persiste los DataFrames procesados hacia el modelo dimensional
alojado en Azure SQL Database utilizando SQLAlchemy y pyodbc.

Esquema destino (Estrella):
    - DIM_TIEMPO: Dimension conforme de calendario (id_tiempo formato YYYYMMDD)
    - DIM_PLATAFORMA: Dimension conforme de origenes de datos
    - DIM_EJECUCION: Dimension de auditoria y trazabilidad (linaje)
    - DIM_CUENTA: Cuentas de Instagram y canales de YouTube
    - DIM_PUBLICACION: Posts, Reels y Videos
    - HECHOS_METRICAS_PUBLICACION: Tabla de hechos ancha con granularidad diaria

USO
---
    python src/load/cargar_datos.py
    python src/load/cargar_datos.py --solo youtube
    python src/load/cargar_datos.py --solo instagram
"""

import argparse
import datetime as dt
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import pandas as pd
except ImportError:
    sys.exit("Falta la libreria 'pandas'. Instala con: pip install -r requirements.txt")

try:
    import pyodbc
except ImportError:
    sys.exit("Falta la libreria 'pyodbc'. Instala con: pip install -r requirements.txt")

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError:
    sys.exit("Falta la libreria 'SQLAlchemy'. Instala con: pip install -r requirements.txt")

from dotenv import load_dotenv

# Importar modulos del pipeline
RAIZ = Path(__file__).resolve().parents[2]
sys.path.append(str(RAIZ))

from src.transform.cargar_json import (
    cargar_todo_instagram,
    cargar_todo_youtube,
)
from src.transform.normalizar import (
    normalizar_instagram,
    normalizar_youtube,
)

load_dotenv(RAIZ / ".env")

log = logging.getLogger("cargar_datos")

# =============================================================================
# Configuracion de Conexion
# =============================================================================

SERVER = os.getenv("AZURE_SERVER")
DATABASE = os.getenv("AZURE_DATABASE")
USER = os.getenv("AZURE_USER")
PASSWORD = os.getenv("AZURE_PASSWORD")


def obtener_engine() -> Engine:
    """
    Crea el motor de SQLAlchemy conectado a Azure SQL Database
    usando pyodbc con ODBC Driver 18 for SQL Server y fast_executemany.
    """
    if not all([SERVER, DATABASE, USER, PASSWORD]):
        raise ValueError("Faltan variables de Azure en el archivo .env")

    drivers = pyodbc.drivers()
    driver_elegido = "ODBC Driver 18 for SQL Server"
    if driver_elegido not in drivers:
        for d in ["ODBC Driver 17 for SQL Server", "SQL Server"]:
            if d in drivers:
                driver_elegido = d
                break

    conn_str = (
        f"DRIVER={{{driver_elegido}}};"
        f"SERVER=tcp:{SERVER},1433;"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    def _conectar():
        for intento in range(1, 4):
            try:
                return pyodbc.connect(conn_str, timeout=30)
            except Exception:
                if intento == 3:
                    raise
                time.sleep(2)

    return create_engine(
        "mssql+pyodbc://",
        creator=_conectar,
        fast_executemany=True,
    )


# =============================================================================
# 1. Carga de DIM_TIEMPO
# =============================================================================

NOMBRES_MES = [
    "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
]
NOMBRES_DIA = [
    "Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"
]


def generar_filas_tiempo(fechas: Set[dt.date]) -> pd.DataFrame:
    """Genera registros para DIM_TIEMPO a partir de un conjunto de fechas."""
    filas = []
    for f in sorted(fechas):
        id_tiempo = int(f.strftime("%Y%m%d"))
        dia_semana_num = f.weekday() + 1  # 1=Lunes .. 7=Domingo
        filas.append({
            "id_tiempo": id_tiempo,
            "fecha": f,
            "anio": f.year,
            "mes": f.month,
            "nombre_mes": NOMBRES_MES[f.month],
            "dia": f.day,
            "dia_semana": dia_semana_num,
            "nombre_dia": NOMBRES_DIA[f.weekday()],
            "trimestre": (f.month - 1) // 3 + 1,
            "es_fin_de_semana": 1 if dia_semana_num in (6, 7) else 0,
        })
    return pd.DataFrame(filas)


def asegurar_fechas_en_dim_tiempo(engine: Engine, fechas: Set[dt.date]) -> int:
    """
    Verifica que las fechas existan en DIM_TIEMPO; si faltan, las inserta.
    Retorna la cantidad de fechas nuevas insertadas.
    """
    if not fechas:
        return 0

    ids_buscados = [int(f.strftime("%Y%m%d")) for f in fechas]
    id_list_str = ",".join(str(i) for i in ids_buscados)

    with engine.connect() as conn:
        query = text(f"SELECT id_tiempo FROM dbo.DIM_TIEMPO WHERE id_tiempo IN ({id_list_str})")
        existentes = set(r[0] for r in conn.execute(query).fetchall())

    faltantes = {f for f in fechas if int(f.strftime("%Y%m%d")) not in existentes}
    if not faltantes:
        return 0

    df_nuevos = generar_filas_tiempo(faltantes)
    df_nuevos.to_sql("DIM_TIEMPO", con=engine, if_exists="append", index=False)
    log.info("  [DIM_TIEMPO] %d fechas insertadas exitosamente.", len(df_nuevos))
    return len(df_nuevos)


# =============================================================================
# 2. Carga de DIM_EJECUCION
# =============================================================================

def cargar_dim_ejecucion(engine: Engine, df_ejecucion: pd.DataFrame) -> int:
    """
    Inserta ejecuciones en DIM_EJECUCION ignorando las ya existentes por id_ejecucion.
    """
    if df_ejecucion is None or df_ejecucion.empty:
        return 0

    # Obtener IDs ya registrados
    with engine.connect() as conn:
        res = conn.execute(text("SELECT id_ejecucion FROM dbo.DIM_EJECUCION"))
        existentes = set(r[0] for r in res.fetchall())

    nuevos = df_ejecucion[~df_ejecucion["id_ejecucion"].isin(existentes)].copy()
    if nuevos.empty:
        log.info("  [DIM_EJECUCION] Sin ejecuciones nuevas para insertar.")
        return 0

    columnas_dw = [
        "id_ejecucion", "plataforma", "version_api",
        "fecha_hora_inicio_utc", "fecha_hora_fin_utc", "duracion_segundos",
        "estado", "contenidos_extraidos", "llamadas_api", "paginas_recorridas",
        "cuota_estimada", "error_tipo", "error_codigo", "error_mensaje",
    ]
    cols_a_usar = [c for c in columnas_dw if c in nuevos.columns]
    df_carga = nuevos[cols_a_usar].rename(columns={"plataforma": "id_plataforma"})

    # Deduplicar por id_ejecucion antes de insertar
    df_carga = df_carga.drop_duplicates(subset=["id_ejecucion"])

    # Normalizar tipos para SQL
    for col in ["fecha_hora_inicio_utc", "fecha_hora_fin_utc"]:
        if col in df_carga.columns:
            df_carga[col] = pd.to_datetime(df_carga[col], errors="coerce", utc=True)
            # Quitar tz para que el driver TDS inserte DATETIME2 directamente en UTC
            df_carga[col] = df_carga[col].dt.tz_convert(None)

    df_carga.to_sql("DIM_EJECUCION", con=engine, if_exists="append", index=False)
    log.info("  [DIM_EJECUCION] %d ejecucion(es) insertada(s).", len(df_carga))
    return len(df_carga)


# =============================================================================
# 3. Carga de DIM_CUENTA
# =============================================================================

def cargar_dim_cuenta(engine: Engine, df_cuenta: pd.DataFrame) -> Tuple[int, Dict[Tuple[str, str], int]]:
    """
    Inserta cuentas no registradas en DIM_CUENTA.
    Retorna (cantidad_insertadas, mapa_claves_sk: {(plataforma, id_externo): id_cuenta_sk}).
    """
    if df_cuenta is None or df_cuenta.empty:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT id_plataforma, id_cuenta_externo, id_cuenta_sk FROM dbo.DIM_CUENTA"))
            return 0, {(r[0], r[1]): r[2] for r in res.fetchall()}

    with engine.connect() as conn:
        res = conn.execute(text("SELECT id_plataforma, id_cuenta_externo, id_cuenta_sk FROM dbo.DIM_CUENTA"))
        mapa_sk = {(r[0], r[1]): r[2] for r in res.fetchall()}

    df_dedup = df_cuenta.drop_duplicates(subset=["plataforma", "id_cuenta"]).copy()
    nuevos = df_dedup[~df_dedup.apply(lambda r: (r["plataforma"], str(r["id_cuenta"])) in mapa_sk, axis=1)]

    insertadas = 0
    if not nuevos.empty:
        df_insert = pd.DataFrame({
            "id_cuenta_externo": nuevos["id_cuenta"].astype(str),
            "id_plataforma": nuevos["plataforma"].astype(str),
            "usuario": nuevos.get("usuario"),
            "nombre": nuevos.get("nombre"),
            "fecha_hora_creacion_utc": pd.to_datetime(nuevos.get("fecha_hora_creacion_utc"), errors="coerce", utc=True).dt.tz_convert(None),
        })
        df_insert.to_sql("DIM_CUENTA", con=engine, if_exists="append", index=False)
        insertadas = len(df_insert)
        log.info("  [DIM_CUENTA] %d cuenta(s) nueva(s) insertada(s).", insertadas)

        # Actualizar mapa con los nuevos IDs asignados por la base de datos
        with engine.connect() as conn:
            res = conn.execute(text("SELECT id_plataforma, id_cuenta_externo, id_cuenta_sk FROM dbo.DIM_CUENTA"))
            mapa_sk = {(r[0], r[1]): r[2] for r in res.fetchall()}
    else:
        log.info("  [DIM_CUENTA] Sin cuentas nuevas para registrar.")

    return insertadas, mapa_sk


# =============================================================================
# 4. Carga de DIM_PUBLICACION
# =============================================================================

def cargar_dim_publicacion(
    engine: Engine,
    df_contenidos: pd.DataFrame,
    mapa_cuentas: Dict[Tuple[str, str], int],
    id_cuenta_externo_por_defecto: Optional[str] = None
) -> Tuple[int, Dict[Tuple[str, str], int]]:
    """
    Inserta publicaciones no registradas en DIM_PUBLICACION.
    Retorna (cantidad_insertadas, mapa_claves_sk: {(plataforma, id_publicacion_externo): id_publicacion_sk}).
    """
    with engine.connect() as conn:
        res = conn.execute(text("SELECT id_plataforma, id_publicacion_externo, id_publicacion_sk FROM dbo.DIM_PUBLICACION"))
        mapa_pub = {(r[0], r[1]): r[2] for r in res.fetchall()}

    if df_contenidos is None or df_contenidos.empty:
        return 0, mapa_pub

    df_dedup = df_contenidos.drop_duplicates(subset=["plataforma", "id_contenido"]).copy()
    nuevos = df_dedup[~df_dedup.apply(lambda r: (r["plataforma"], str(r["id_contenido"])) in mapa_pub, axis=1)]

    insertadas = 0
    if not nuevos.empty:
        filas = []
        for _, row in nuevos.iterrows():
            plat = str(row["plataforma"])
            # Resolver id_cuenta_sk
            cta_ext = str(row.get("id_cuenta", id_cuenta_externo_por_defecto or ""))
            cuenta_sk = mapa_cuentas.get((plat, cta_ext))
            if not cuenta_sk:
                # Si no viene explicito en el registro, asociar a la primera cuenta de esa plataforma
                for (p, _), c_sk in mapa_cuentas.items():
                    if p == plat:
                        cuenta_sk = c_sk
                        break

            f_pub = pd.to_datetime(row.get("fecha_hora_publicacion_utc"), errors="coerce", utc=True)
            if pd.notna(f_pub):
                f_pub = f_pub.tz_convert(None)
            else:
                f_pub = None

            filas.append({
                "id_publicacion_externo": str(row["id_contenido"]),
                "id_plataforma": plat,
                "id_cuenta_sk": cuenta_sk,
                "titulo": str(row["titulo"])[:500] if pd.notna(row.get("titulo")) else None,
                "texto_descripcion": str(row.get("texto")) if pd.notna(row.get("texto")) else None,
                "tipo_medio": str(row.get("tipo_medio"))[:50] if pd.notna(row.get("tipo_medio")) else None,
                "tipo_producto": str(row.get("tipo_producto"))[:50] if pd.notna(row.get("tipo_producto")) else None,
                "url_permalink": str(row.get("url"))[:500] if pd.notna(row.get("url")) else None,
                "privacidad": str(row.get("privacidad"))[:50] if pd.notna(row.get("privacidad")) else None,
                "duracion_segundos": int(row["duracion_segundos"]) if pd.notna(row.get("duracion_segundos")) else None,
                "fecha_hora_publicacion_utc": f_pub,
            })

        df_insert = pd.DataFrame(filas)
        df_insert.to_sql("DIM_PUBLICACION", con=engine, if_exists="append", index=False)
        insertadas = len(df_insert)
        log.info("  [DIM_PUBLICACION] %d publicacion(es) insertada(s).", insertadas)

        # Actualizar mapa
        with engine.connect() as conn:
            res = conn.execute(text("SELECT id_plataforma, id_publicacion_externo, id_publicacion_sk FROM dbo.DIM_PUBLICACION"))
            mapa_pub = {(r[0], r[1]): r[2] for r in res.fetchall()}
    else:
        log.info("  [DIM_PUBLICACION] Sin publicaciones nuevas.")

    return insertadas, mapa_pub


# =============================================================================
# 5. Carga de HECHOS_METRICAS_PUBLICACION
# =============================================================================

def cargar_hechos_youtube(
    engine: Engine,
    tablas_yt: Dict[str, pd.DataFrame],
    mapa_cuentas: Dict[Tuple[str, str], int],
    mapa_publicaciones: Dict[Tuple[str, str], int],
) -> int:
    """Consolida e inserta observaciones de YouTube en la tabla de hechos."""
    estadisticas = tablas_yt.get("estadisticas")
    if estadisticas is None or estadisticas.empty:
        return 0

    ejecucion = tablas_yt.get("ejecucion")
    fecha_obs = dt.date.today()
    if ejecucion is not None and not ejecucion.empty:
        f_ini = pd.to_datetime(ejecucion["fecha_hora_inicio_utc"].iloc[0], errors="coerce", utc=True)
        if pd.notna(f_ini):
            fecha_obs = f_ini.date()

    id_tiempo = int(fecha_obs.strftime("%Y%m%d"))
    asegurar_fechas_en_dim_tiempo(engine, {fecha_obs})

    # Verificar hechos ya insertados para evitar violacion de clave unica
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT id_publicacion_sk, id_tiempo, id_ejecucion FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_tiempo = {id_tiempo}"))
        hechos_existentes = set((r[0], r[1], str(r[2])) for r in res.fetchall())

    filas_hechos = []
    for _, row in estadisticas.iterrows():
        vid_id = str(row["id_contenido"])
        pub_sk = mapa_publicaciones.get(("youtube", vid_id))
        id_ejec = str(row.get("id_ejecucion", ""))
        
        # Resolver cuenta
        cuenta_sk = None
        for (plat, _), c_sk in mapa_cuentas.items():
            if plat == "youtube":
                cuenta_sk = c_sk
                break

        if not pub_sk or not cuenta_sk:
            continue

        clave_hecho = (pub_sk, id_tiempo, id_ejec)
        if clave_hecho in hechos_existentes:
            continue

        filas_hechos.append({
            "id_tiempo": id_tiempo,
            "id_plataforma": "youtube",
            "id_cuenta_sk": cuenta_sk,
            "id_publicacion_sk": pub_sk,
            "id_ejecucion": id_ejec,
            "cantidad_vistas": int(row["cantidad_vistas"]) if pd.notna(row.get("cantidad_vistas")) else None,
            "cantidad_me_gusta": int(row["cantidad_me_gusta"]) if pd.notna(row.get("cantidad_me_gusta")) else None,
            "cantidad_comentarios": int(row["cantidad_comentarios"]) if pd.notna(row.get("cantidad_comentarios")) else None,
            "cantidad_compartidos": None,
            "cantidad_guardados": None,
            "alcance": None,
            "impresiones": None,
        })

    if not filas_hechos:
        return 0

    df_hechos = pd.DataFrame(filas_hechos)
    df_hechos.to_sql("HECHOS_METRICAS_PUBLICACION", con=engine, if_exists="append", index=False)
    log.info("  [HECHOS_METRICAS_PUBLICACION] %d observaciones de YouTube insertadas.", len(df_hechos))
    return len(df_hechos)


def cargar_hechos_instagram(
    engine: Engine,
    tablas_ig: Dict[str, pd.DataFrame],
    mapa_cuentas: Dict[Tuple[str, str], int],
    mapa_publicaciones: Dict[Tuple[str, str], int],
) -> int:
    """Consolida e inserta observaciones de Instagram en la tabla de hechos."""
    contenidos = tablas_ig.get("contenidos")
    if contenidos is None or contenidos.empty:
        return 0

    metricas = tablas_ig.get("metricas")
    ejecucion = tablas_ig.get("ejecucion")
    fecha_obs = dt.date.today()
    if ejecucion is not None and not ejecucion.empty:
        f_ini = pd.to_datetime(ejecucion["fecha_hora_inicio_utc"].iloc[0], errors="coerce", utc=True)
        if pd.notna(f_ini):
            fecha_obs = f_ini.date()

    id_tiempo = int(fecha_obs.strftime("%Y%m%d"))
    asegurar_fechas_en_dim_tiempo(engine, {fecha_obs})

    # Pivotear metricas si existen
    metricas_pivot = {}
    if metricas is not None and not metricas.empty:
        for _, m_row in metricas.iterrows():
            cid = str(m_row["id_contenido"])
            m_nom = str(m_row["metrica"]).lower()
            val = int(m_row["valor"]) if pd.notna(m_row.get("valor")) else None
            if cid not in metricas_pivot:
                metricas_pivot[cid] = {}
            metricas_pivot[cid][m_nom] = val

    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT id_publicacion_sk, id_tiempo, id_ejecucion FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_tiempo = {id_tiempo}"))
        hechos_existentes = set((r[0], r[1], str(r[2])) for r in res.fetchall())

    cuenta_sk = None
    for (plat, _), c_sk in mapa_cuentas.items():
        if plat == "instagram":
            cuenta_sk = c_sk
            break

    filas_hechos = []
    for _, row in contenidos.iterrows():
        media_id = str(row["id_contenido"])
        pub_sk = mapa_publicaciones.get(("instagram", media_id))
        id_ejec = str(row.get("id_ejecucion", ""))

        if not pub_sk or not cuenta_sk:
            continue

        clave_hecho = (pub_sk, id_tiempo, id_ejec)
        if clave_hecho in hechos_existentes:
            continue

        insights = metricas_pivot.get(media_id, {})

        filas_hechos.append({
            "id_tiempo": id_tiempo,
            "id_plataforma": "instagram",
            "id_cuenta_sk": cuenta_sk,
            "id_publicacion_sk": pub_sk,
            "id_ejecucion": id_ejec,
            "cantidad_vistas": None,
            "cantidad_me_gusta": int(row["cantidad_me_gusta"]) if pd.notna(row.get("cantidad_me_gusta")) else None,
            "cantidad_comentarios": int(row["cantidad_comentarios"]) if pd.notna(row.get("cantidad_comentarios")) else None,
            "cantidad_compartidos": insights.get("shares"),
            "cantidad_guardados": insights.get("saved"),
            "alcance": insights.get("reach"),
            "impresiones": insights.get("impressions"),
        })

    if not filas_hechos:
        return 0

    df_hechos = pd.DataFrame(filas_hechos)
    df_hechos.to_sql("HECHOS_METRICAS_PUBLICACION", con=engine, if_exists="append", index=False)
    log.info("  [HECHOS_METRICAS_PUBLICACION] %d observaciones de Instagram insertadas.", len(df_hechos))
    return len(df_hechos)


# =============================================================================
# Orquestador del Proceso de Carga
# =============================================================================

def cargar_plataforma(
    engine: Engine,
    tablas_norm: Dict[str, pd.DataFrame],
    plataforma: str,
) -> Dict[str, int]:
    """Carga todas las tablas de una plataforma hacia el Data Warehouse."""
    resumen = {}
    log.info("\n>>> Iniciando carga para %s <<<", plataforma.upper())

    # 1. Dim Tiempo (extraer fechas de ejecucion y publicaciones)
    fechas = set()
    ejecucion = tablas_norm.get("ejecucion")
    if ejecucion is not None and not ejecucion.empty:
        for f in ejecucion["fecha_hora_inicio_utc"].dropna():
            fechas.add(pd.to_datetime(f, utc=True).date())

    contenidos = tablas_norm.get("contenidos")
    if contenidos is not None and not contenidos.empty:
        for f in contenidos["fecha_hora_publicacion_utc"].dropna():
            fechas.add(pd.to_datetime(f, utc=True).date())

    resumen["DIM_TIEMPO"] = asegurar_fechas_en_dim_tiempo(engine, fechas)

    # 2. Dim Ejecucion
    resumen["DIM_EJECUCION"] = cargar_dim_ejecucion(engine, ejecucion)

    # 3. Dim Cuenta
    cta_ins, mapa_cuentas = cargar_dim_cuenta(engine, tablas_norm.get("cuenta"))
    resumen["DIM_CUENTA"] = cta_ins

    # 4. Dim Publicacion
    pub_ins, mapa_publicaciones = cargar_dim_publicacion(engine, contenidos, mapa_cuentas)
    resumen["DIM_PUBLICACION"] = pub_ins

    # 5. Hechos
    if plataforma == "youtube":
        resumen["HECHOS"] = cargar_hechos_youtube(engine, tablas_norm, mapa_cuentas, mapa_publicaciones)
    elif plataforma == "instagram":
        resumen["HECHOS"] = cargar_hechos_instagram(engine, tablas_norm, mapa_cuentas, mapa_publicaciones)

    return resumen


def imprimir_resumen_carga(resumenes: Dict[str, Dict[str, int]]):
    print("\n" + "=" * 60)
    print("  RESUMEN DE CARGA AL DATA WAREHOUSE (AZURE SQL)")
    print("=" * 60)
    print(f"  {'Tabla Destino':<28} {'Instagram':>12} {'YouTube':>12}")
    print("  " + "-" * 54)

    tablas = ["DIM_TIEMPO", "DIM_EJECUCION", "DIM_CUENTA", "DIM_PUBLICACION", "HECHOS"]
    for t in tablas:
        cant_ig = resumenes.get("instagram", {}).get(t, 0)
        cant_yt = resumenes.get("youtube", {}).get(t, 0)
        print(f"  {t:<28} {cant_ig:>12} {cant_yt:>12}")
    print("=" * 60)


def configurar_logging():
    manejador = logging.StreamHandler(sys.stdout)
    manejador.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    logging.basicConfig(level=logging.INFO, handlers=[manejador])


def parsear_argumentos(argumentos=None):
    parser = argparse.ArgumentParser(description="Carga datos transformados hacia Azure SQL Database.")
    parser.add_argument("--solo", choices=["instagram", "youtube"], help="Cargar unicamente una plataforma")
    return parser.parse_args(argumentos)


def main(argumentos=None):
    args = parsear_argumentos(argumentos)
    configurar_logging()

    print("\n" + "=" * 60)
    print("  PIPELINE ETL - FASE DE CARGA (LOAD) HACIA AZURE SQL")
    print(f"  Fecha: {dt.datetime.now():%d-%m-%Y %H:%M}")
    print("=" * 60)

    engine = obtener_engine()
    for intento in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("[OK] Conexion con Azure SQL Database establecida.")
            break
        except Exception as e:
            if intento == 3:
                sys.exit(f"[!] Error critico conectando con Azure SQL: {e}")
            log.info("  Reintentando conexion con Azure SQL...")
            time.sleep(3)

    resumenes = {}

    # Cargar Instagram
    if args.solo is None or args.solo == "instagram":
        tablas_crudo_ig = cargar_todo_instagram()
        tablas_norm_ig = normalizar_instagram(tablas_crudo_ig)
        resumenes["instagram"] = cargar_plataforma(engine, tablas_norm_ig, "instagram")

    # Cargar YouTube
    if args.solo is None or args.solo == "youtube":
        tablas_crudo_yt = cargar_todo_youtube()
        tablas_norm_yt = normalizar_youtube(tablas_crudo_yt)
        resumenes["youtube"] = cargar_plataforma(engine, tablas_norm_yt, "youtube")

    imprimir_resumen_carga(resumenes)
    print("\n[OK] Proceso de carga finalizado exitosamente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
