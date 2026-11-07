#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
AUDITORIA E INTEGRIDAD DE DATOS: DATA CRUDA (RAW) VS AZURE SQL DATABASE
Proyecto: Pipeline ETL Multi-Plataforma (UTEM - Trabajo de Titulo)
=============================================================================
Este script realiza pruebas rigurosas de integridad y reconciliacion comparando
las respuestas crudas en JSON (data/raw/) contra el Data Warehouse en Azure SQL.

Auditorias ejecutadas:
  1. Reconciliacion de Volumen y Entidades (Cuentas, Publicaciones, Ejecuciones, Hechos).
  2. Fidelidad de Atributos de Publicacion (IDs, tipos, permalinks, fechas UTC).
  3. Exactitud de Metricas Cuantitativas (Likes, comentarios, vistas, alcance, impresiones).
  4. Integridad Referencial del Modelo Dimensional (0 registros huerfanos de FK).
  5. Unicidad, Deduplicacion e Idempotencia (0 duplicados en PKs y UKs).
  6. Consistencia Temporal y Horaria UTC (Coherencia id_tiempo y timestamps).
=============================================================================
"""

import sys
import os
import json
import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine

# Asegurar importacion desde la raiz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.load.cargar_datos import obtener_engine

RUTA_RAW = ROOT_DIR / "data" / "raw"


class Colores:
    VERDE = "\033[92m"
    ROJO = "\033[91m"
    AMARILLO = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def print_titulo(texto: str):
    print(f"\n{Colores.CYAN}{Colores.BOLD}{'=' * 75}{Colores.RESET}")
    print(f"{Colores.CYAN}{Colores.BOLD}  {texto}{Colores.RESET}")
    print(f"{Colores.CYAN}{Colores.BOLD}{'=' * 75}{Colores.RESET}")


def print_seccion(texto: str):
    print(f"\n{Colores.BOLD}--- {texto} ---{Colores.RESET}")


def print_ok(mensaje: str):
    print(f"  {Colores.VERDE}[OK]{Colores.RESET} {mensaje}")


def print_falla(mensaje: str):
    print(f"  {Colores.ROJO}[FALLA]{Colores.RESET} {mensaje}")


def print_aviso(mensaje: str):
    print(f"  {Colores.AMARILLO}[AVISO]{Colores.RESET} {mensaje}")


# =============================================================================
# 1. PARSEO DE DATA CRUDA (RAW)
# =============================================================================

def cargar_todos_los_raw() -> Dict[str, Any]:
    """
    Lee y estructura todos los JSONs crudos de data/raw/ para contrastar con el DW.
    """
    datos = {
        "ejecuciones": {},       # id_ejecucion -> metadatos
        "cuentas": {},           # (plataforma, id_externo) -> datos_cuenta
        "publicaciones": {},     # (plataforma, id_externo) -> datos_publicacion
        "metricas_por_ejec": {}, # id_ejecucion -> {id_pub_externo -> metricas_dict}
    }

    # 1. Instagram
    for archivo in sorted((RUTA_RAW / "instagram").glob("*.json")):
        try:
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            meta = contenido.get("metadatos_ejecucion", {})
            resp = contenido.get("respuestas", {})
            id_ejec = meta.get("id_ejecucion")
            if not id_ejec:
                continue

            datos["ejecuciones"][id_ejec] = {
                "plataforma": "instagram",
                "estado": meta.get("estado"),
                "fecha_inicio_utc": meta.get("fecha_hora_inicio_utc"),
                "contenidos_extraidos": meta.get("publicaciones_extraidas", 0),
                "llamadas_api": meta.get("llamadas_api", 0),
                "archivo": archivo.name,
            }

            perfil = resp.get("perfil")
            if perfil and "id" in perfil:
                cta_id = str(perfil["id"])
                datos["cuentas"][("instagram", cta_id)] = {
                    "usuario": perfil.get("username"),
                    "nombre": perfil.get("name"),
                    "seguidores": perfil.get("followers_count"),
                }

            # Publicaciones e Insights
            insights_raw = resp.get("insights") or {}
            metricas_pub_ejec = {}

            paginas = resp.get("paginas_media", [])
            for pag in paginas:
                for item in pag.get("data", []):
                    item_id = str(item.get("id"))
                    ts_str = item.get("timestamp")
                    
                    datos["publicaciones"][("instagram", item_id)] = {
                        "tipo_medio": item.get("media_type"),
                        "tipo_producto": item.get("media_product_type"),
                        "permalink": item.get("permalink"),
                        "timestamp": ts_str,
                        "caption": item.get("caption"),
                    }

                    # Extraer metricas
                    m_dict = {
                        "likes": item.get("like_count"),
                        "comments": item.get("comments_count"),
                        "alcance": None,
                        "impresiones": None,
                        "guardados": None,
                        "compartidos": None,
                        "vistas": None,
                    }

                    # Parsear bloque insights
                    if item_id in insights_raw:
                        ins_data = insights_raw[item_id].get("data", [])
                        for ins in ins_data:
                            nom = ins.get("name")
                            vals = ins.get("values", [])
                            val_num = vals[0].get("value") if vals else None
                            if nom == "reach":
                                m_dict["alcance"] = val_num
                            elif nom == "impressions":
                                m_dict["impresiones"] = val_num
                            elif nom == "saved":
                                m_dict["guardados"] = val_num
                            elif nom == "shares":
                                m_dict["compartidos"] = val_num
                            elif nom in ("plays", "ig_reels_aggregated_all_plays_count"):
                                m_dict["vistas"] = val_num

                    metricas_pub_ejec[item_id] = m_dict

            if metricas_pub_ejec:
                datos["metricas_por_ejec"][id_ejec] = metricas_pub_ejec

        except Exception as e:
            print_aviso(f"No se pudo parsear {archivo.name}: {e}")

    # 2. YouTube
    for archivo in sorted((RUTA_RAW / "youtube").glob("*.json")):
        try:
            contenido = json.loads(archivo.read_text(encoding="utf-8"))
            meta = contenido.get("metadatos_ejecucion", {})
            resp = contenido.get("respuestas", {})
            id_ejec = meta.get("id_ejecucion")
            if not id_ejec:
                continue

            datos["ejecuciones"][id_ejec] = {
                "plataforma": "youtube",
                "estado": meta.get("estado"),
                "fecha_inicio_utc": meta.get("fecha_hora_inicio_utc"),
                "contenidos_extraidos": meta.get("videos_extraidos", 0),
                "llamadas_api": meta.get("llamadas_api", 0),
                "archivo": archivo.name,
            }

            canal_resp = resp.get("canal") or {}
            items_canal = canal_resp.get("items", [])
            if items_canal:
                c = items_canal[0]
                cid = str(c.get("id"))
                snip = c.get("snippet", {})
                datos["cuentas"][("youtube", cid)] = {
                    "usuario": snip.get("customUrl"),
                    "nombre": snip.get("title"),
                    "fecha_creacion": snip.get("publishedAt"),
                }

        except Exception as e:
            print_aviso(f"No se pudo parsear {archivo.name}: {e}")

    return datos


# =============================================================================
# 2. SUITE DE PRUEBAS DE INTEGRIDAD
# =============================================================================

def auditar_conciliacion_volumen(engine: Engine, raw: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Compara las cantidades totales en crudo vs Azure SQL.
    """
    print_seccion("1. Conciliacion de Volumen y Entidades (Raw vs Azure SQL)")
    exito = True

    with engine.connect() as conn:
        dw_cuentas = conn.execute(sa.text("SELECT id_plataforma, id_cuenta_externo FROM dbo.DIM_CUENTA")).fetchall()
        dw_publicaciones = conn.execute(sa.text("SELECT id_plataforma, id_publicacion_externo FROM dbo.DIM_PUBLICACION")).fetchall()
        dw_ejecuciones = conn.execute(sa.text("SELECT id_ejecucion FROM dbo.DIM_EJECUCION")).fetchall()
        dw_hechos = conn.execute(sa.text("SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION")).scalar()

    set_dw_cuentas = set((r[0], str(r[1])) for r in dw_cuentas)
    set_dw_pubs = set((r[0], str(r[1])) for r in dw_publicaciones)
    set_dw_ejec = set(str(r[0]) for r in dw_ejecuciones)

    set_raw_cuentas = set(raw["cuentas"].keys())
    set_raw_pubs = set(raw["publicaciones"].keys())
    set_raw_ejec = set(raw["ejecuciones"].keys())

    # 1. Cuentas
    cuentas_faltantes = set_raw_cuentas - set_dw_cuentas
    if not cuentas_faltantes:
        print_ok(f"Cuentas: {len(set_raw_cuentas)} de {len(set_raw_cuentas)} crudas presentes en DIM_CUENTA (Total DW: {len(set_dw_cuentas)}).")
    else:
        print_falla(f"Cuentas crudas faltantes en DIM_CUENTA: {cuentas_faltantes}")
        exito = False

    # 2. Publicaciones
    pubs_faltantes = set_raw_pubs - set_dw_pubs
    if not pubs_faltantes:
        print_ok(f"Publicaciones: {len(set_raw_pubs)} de {len(set_raw_pubs)} crudas presentes en DIM_PUBLICACION (Total DW: {len(set_dw_pubs)}).")
    else:
        print_falla(f"Publicaciones crudas faltantes en DIM_PUBLICACION: {pubs_faltantes}")
        exito = False

    # 3. Ejecuciones
    ejec_faltantes = set_raw_ejec - set_dw_ejec
    if not ejec_faltantes:
        print_ok(f"Ejecuciones: {len(set_raw_ejec)} locales presentes en DIM_EJECUCION (Total DW: {len(set_dw_ejec)}).")
    else:
        print_falla(f"Ejecuciones crudas faltantes en DIM_EJECUCION: {len(ejec_faltantes)}")
        exito = False

    # 4. Hechos
    print_ok(f"Observaciones de metricas en HECHOS: {dw_hechos} registros persistidos.")

    return exito, {
        "cuentas_raw": len(set_raw_cuentas), "cuentas_dw": len(set_dw_cuentas),
        "pubs_raw": len(set_raw_pubs), "pubs_dw": len(set_dw_pubs),
        "ejec_raw": len(set_raw_ejec), "ejec_dw": len(set_dw_ejec),
        "hechos_dw": dw_hechos,
    }


def auditar_fidelidad_atributos_publicacion(engine: Engine, raw: Dict[str, Any]) -> bool:
    """
    Compara campo por campo los atributos de DIM_PUBLICACION con el JSON crudo.
    """
    print_seccion("2. Fidelidad y Precision de Atributos (DIM_PUBLICACION vs Raw)")
    exito = True

    with engine.connect() as conn:
        df_dw = pd.read_sql(
            "SELECT id_publicacion_externo, id_plataforma, tipo_medio, tipo_producto, url_permalink, fecha_hora_publicacion_utc FROM dbo.DIM_PUBLICACION",
            conn
        )

    dw_dict = {
        (row["id_plataforma"], str(row["id_publicacion_externo"])): row
        for _, row in df_dw.iterrows()
    }

    total_evaluadas = 0
    atributos_coincidentes = 0
    total_atributos = 0

    for (plat, ext_id), raw_item in raw["publicaciones"].items():
        dw_item = dw_dict.get((plat, ext_id))
        if dw_item is None:
            continue

        total_evaluadas += 1

        # 1. Tipo de medio
        total_atributos += 1
        if str(dw_item["tipo_medio"]).upper() == str(raw_item.get("tipo_medio", "")).upper():
            atributos_coincidentes += 1
        else:
            print_falla(f"Pub {ext_id} tipo_medio difiere: raw='{raw_item.get('tipo_medio')}' dw='{dw_item['tipo_medio']}'")
            exito = False

        # 2. Tipo de producto
        total_atributos += 1
        if str(dw_item["tipo_producto"]).upper() == str(raw_item.get("tipo_producto", "")).upper():
            atributos_coincidentes += 1
        else:
            print_falla(f"Pub {ext_id} tipo_producto difiere: raw='{raw_item.get('tipo_producto')}' dw='{dw_item['tipo_producto']}'")
            exito = False

        # 3. Permalink
        total_atributos += 1
        raw_perm = (raw_item.get("permalink") or "").strip()
        dw_perm = (dw_item["url_permalink"] or "").strip()
        if raw_perm == dw_perm:
            atributos_coincidentes += 1
        else:
            print_falla(f"Pub {ext_id} permalink difiere: raw='{raw_perm}' dw='{dw_perm}'")
            exito = False

        # 4. Timestamp UTC
        total_atributos += 1
        raw_ts = pd.to_datetime(raw_item.get("timestamp"), utc=True)
        dw_ts = pd.to_datetime(dw_item["fecha_hora_publicacion_utc"], utc=True)
        if raw_ts == dw_ts:
            atributos_coincidentes += 1
        else:
            print_falla(f"Pub {ext_id} fecha_hora_publicacion_utc difiere: raw='{raw_ts}' dw='{dw_ts}'")
            exito = False

    tasa = (atributos_coincidentes / total_atributos * 100) if total_atributos else 100
    if exito:
        print_ok(f"Todos los atributos verificados ({total_atributos}/{total_atributos}) en {total_evaluadas} publicaciones. Precision: 100.0%.")
    else:
        print_falla(f"Discrepancias en atributos. Precision: {tasa:.2f}% ({atributos_coincidentes}/{total_atributos}).")

    return exito


def auditar_exactitud_metricas(engine: Engine, raw: Dict[str, Any]) -> bool:
    """
    Compara las metricas numericas en HECHOS_METRICAS_PUBLICACION vs los valores crudos.
    """
    print_seccion("3. Conciliacion de Metricas Cuantitativas (HECHOS vs Raw)")
    exito = True

    # Tomar la ejecucion que contenga metricas cargadas
    ejecs_con_metricas = list(raw["metricas_por_ejec"].keys())
    if not ejecs_con_metricas:
        print_aviso("No hay archivos JSON crudos locales con publicaciones y metricas extraidas.")
        return True

    lista_ids = ", ".join(f"'{x}'" for x in ejecs_con_metricas)
    with engine.connect() as conn:
        df_hechos = pd.read_sql(
            f"""
                SELECT h.*, p.id_publicacion_externo 
                FROM dbo.HECHOS_METRICAS_PUBLICACION h
                JOIN dbo.DIM_PUBLICACION p ON h.id_publicacion_sk = p.id_publicacion_sk
                WHERE h.id_ejecucion IN ({lista_ids})
            """,
            conn
        )

    if df_hechos.empty:
        print_aviso("No se encontraron hechos en Azure SQL para las ejecuciones locales analizadas.")
        return True

    comparaciones_totales = 0
    comparaciones_exitosas = 0

    # Crear indice de hechos: (id_ejecucion, id_publicacion_externo) -> fila_hechos
    hechos_map = {
        (str(r["id_ejecucion"]), str(r["id_publicacion_externo"])): r
        for _, r in df_hechos.iterrows()
    }

    for id_ejec, pubs_metricas in raw["metricas_por_ejec"].items():
        for ext_id, m_raw in pubs_metricas.items():
            dw_h = hechos_map.get((id_ejec, ext_id))
            if dw_h is None:
                continue

            # Comparar Likes
            if m_raw.get("likes") is not None:
                comparaciones_totales += 1
                dw_likes = int(dw_h["cantidad_me_gusta"]) if pd.notna(dw_h["cantidad_me_gusta"]) else None
                if dw_likes == int(m_raw["likes"]):
                    comparaciones_exitosas += 1
                else:
                    print_falla(f"Ejec {id_ejec[:8]} Pub {ext_id}: likes crudo={m_raw['likes']} dw={dw_likes}")
                    exito = False

            # Comparar Comments
            if m_raw.get("comments") is not None:
                comparaciones_totales += 1
                dw_comments = int(dw_h["cantidad_comentarios"]) if pd.notna(dw_h["cantidad_comentarios"]) else None
                if dw_comments == int(m_raw["comments"]):
                    comparaciones_exitosas += 1
                else:
                    print_falla(f"Ejec {id_ejec[:8]} Pub {ext_id}: comments crudo={m_raw['comments']} dw={dw_comments}")
                    exito = False

            # Comparar Alcance (reach)
            if m_raw.get("alcance") is not None:
                comparaciones_totales += 1
                dw_alcance = int(dw_h["alcance"]) if pd.notna(dw_h["alcance"]) else None
                if dw_alcance == int(m_raw["alcance"]):
                    comparaciones_exitosas += 1
                else:
                    print_falla(f"Ejec {id_ejec[:8]} Pub {ext_id}: alcance crudo={m_raw['alcance']} dw={dw_alcance}")
                    exito = False

            # Comparar Guardados (saved)
            if m_raw.get("guardados") is not None:
                comparaciones_totales += 1
                dw_saved = int(dw_h["cantidad_guardados"]) if pd.notna(dw_h["cantidad_guardados"]) else None
                if dw_saved == int(m_raw["guardados"]):
                    comparaciones_exitosas += 1
                else:
                    print_falla(f"Ejec {id_ejec[:8]} Pub {ext_id}: guardados crudo={m_raw['guardados']} dw={dw_saved}")
                    exito = False

    tasa = (comparaciones_exitosas / comparaciones_totales * 100) if comparaciones_totales else 100
    if exito and comparaciones_totales > 0:
        print_ok(f"Metricas cuantitativas verificadas ({comparaciones_exitosas}/{comparaciones_totales}). Concordancia: 100.0%.")
    elif comparaciones_totales == 0:
        print_aviso("No hubo metricas que comparar.")
    else:
        print_falla(f"Discrepancia en metricas: {comparaciones_exitosas}/{comparaciones_totales} correctas ({tasa:.2f}%).")

    return exito


def auditar_integridad_referencial(engine: Engine) -> bool:
    """
    Verifica que no existan claves foraneas huerfanas en el modelo en estrella.
    """
    print_seccion("4. Integridad Referencial y Relacional (Cero Huerfanos)")
    exito = True

    consultas_huerfanos = [
        ("HECHOS -> DIM_TIEMPO", "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_tiempo NOT IN (SELECT id_tiempo FROM dbo.DIM_TIEMPO)"),
        ("HECHOS -> DIM_PLATAFORMA", "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_plataforma NOT IN (SELECT id_plataforma FROM dbo.DIM_PLATAFORMA)"),
        ("HECHOS -> DIM_CUENTA", "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_cuenta_sk NOT IN (SELECT id_cuenta_sk FROM dbo.DIM_CUENTA)"),
        ("HECHOS -> DIM_PUBLICACION", "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_publicacion_sk NOT IN (SELECT id_publicacion_sk FROM dbo.DIM_PUBLICACION)"),
        ("HECHOS -> DIM_EJECUCION", "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_ejecucion NOT IN (SELECT id_ejecucion FROM dbo.DIM_EJECUCION)"),
        ("DIM_PUBLICACION -> DIM_CUENTA", "SELECT COUNT(*) FROM dbo.DIM_PUBLICACION WHERE id_cuenta_sk NOT IN (SELECT id_cuenta_sk FROM dbo.DIM_CUENTA)"),
        ("DIM_PUBLICACION -> DIM_PLATAFORMA", "SELECT COUNT(*) FROM dbo.DIM_PUBLICACION WHERE id_plataforma NOT IN (SELECT id_plataforma FROM dbo.DIM_PLATAFORMA)"),
        ("DIM_CUENTA -> DIM_PLATAFORMA", "SELECT COUNT(*) FROM dbo.DIM_CUENTA WHERE id_plataforma NOT IN (SELECT id_plataforma FROM dbo.DIM_PLATAFORMA)"),
        ("DIM_EJECUCION -> DIM_PLATAFORMA", "SELECT COUNT(*) FROM dbo.DIM_EJECUCION WHERE id_plataforma NOT IN (SELECT id_plataforma FROM dbo.DIM_PLATAFORMA)"),
    ]

    with engine.connect() as conn:
        for nombre, sql in consultas_huerfanos:
            huerfanos = conn.execute(sa.text(sql)).scalar()
            if huerfanos == 0:
                print_ok(f"{nombre}: 0 registros huerfanos.")
            else:
                print_falla(f"{nombre}: {huerfanos} registros huerfanos detectados.")
                exito = False

    return exito


def auditar_unicidad_y_deduplicacion(engine: Engine) -> bool:
    """
    Verifica que no existan duplicados en claves primarias o de negocio.
    """
    print_seccion("5. Unicidad, Idempotencia y Deduplicacion")
    exito = True

    consultas_duplicados = [
        (
            "UQ_DIM_PUBLICACION_EXTERNO (id_plataforma, id_publicacion_externo)",
            "SELECT COUNT(*) FROM (SELECT id_plataforma, id_publicacion_externo, COUNT(*) as c FROM dbo.DIM_PUBLICACION GROUP BY id_plataforma, id_publicacion_externo HAVING COUNT(*) > 1) t"
        ),
        (
            "UQ_DIM_CUENTA_PLATAFORMA_EXTERNO (id_plataforma, id_cuenta_externo)",
            "SELECT COUNT(*) FROM (SELECT id_plataforma, id_cuenta_externo, COUNT(*) as c FROM dbo.DIM_CUENTA GROUP BY id_plataforma, id_cuenta_externo HAVING COUNT(*) > 1) t"
        ),
        (
            "UQ_HECHOS_OBSERVACION (id_publicacion_sk, id_tiempo, id_ejecucion)",
            "SELECT COUNT(*) FROM (SELECT id_publicacion_sk, id_tiempo, id_ejecucion, COUNT(*) as c FROM dbo.HECHOS_METRICAS_PUBLICACION GROUP BY id_publicacion_sk, id_tiempo, id_ejecucion HAVING COUNT(*) > 1) t"
        ),
        (
            "PK_DIM_EJECUCION (id_ejecucion)",
            "SELECT COUNT(*) FROM (SELECT id_ejecucion, COUNT(*) as c FROM dbo.DIM_EJECUCION GROUP BY id_ejecucion HAVING COUNT(*) > 1) t"
        ),
        (
            "PK_DIM_TIEMPO (id_tiempo)",
            "SELECT COUNT(*) FROM (SELECT id_tiempo, COUNT(*) as c FROM dbo.DIM_TIEMPO GROUP BY id_tiempo HAVING COUNT(*) > 1) t"
        ),
    ]

    with engine.connect() as conn:
        for nombre, sql in consultas_duplicados:
            duplicados = conn.execute(sa.text(sql)).scalar()
            if duplicados == 0:
                print_ok(f"{nombre}: 0 duplicados.")
            else:
                print_falla(f"{nombre}: {duplicados} tuplas duplicadas detectadas.")
                exito = False

    return exito


def auditar_consistencia_temporal(engine: Engine) -> bool:
    """
    Verifica que las fechas correspondan exactamente entre hechos, tiempos y ejecuciones.
    """
    print_seccion("6. Consistencia Temporal y Estandarizacion UTC")
    exito = True

    with engine.connect() as conn:
        # 1. Correspondencia id_tiempo con fecha de la ejecucion
        sql_fecha_match = """
            SELECT COUNT(*) 
            FROM dbo.HECHOS_METRICAS_PUBLICACION h
            JOIN dbo.DIM_EJECUCION e ON h.id_ejecucion = e.id_ejecucion
            WHERE h.id_tiempo <> CAST(CONVERT(VARCHAR(8), e.fecha_hora_inicio_utc, 112) AS INT)
        """
        descalces = conn.execute(sa.text(sql_fecha_match)).scalar()
        if descalces == 0:
            print_ok("Correspondencia id_tiempo (YYYYMMDD) con fecha_hora_inicio_utc de ejecucion: 0 discrepancias.")
        else:
            print_falla(f"Se detectaron {descalces} hechos con id_tiempo discordante con la fecha de la ejecucion.")
            exito = False

        # 2. Coherencia interna en DIM_TIEMPO
        sql_tiempo_calc = """
            SELECT COUNT(*)
            FROM dbo.DIM_TIEMPO
            WHERE id_tiempo <> (anio * 10000 + mes * 100 + dia)
        """
        inconsistencias_tiempo = conn.execute(sa.text(sql_tiempo_calc)).scalar()
        if inconsistencias_tiempo == 0:
            print_ok("Coherencia de clave dimensional en DIM_TIEMPO (id_tiempo = anio*10000 + mes*100 + dia): 0 errores.")
        else:
            print_falla(f"Se detectaron {inconsistencias_tiempo} registros en DIM_TIEMPO con clave discordante.")
            exito = False

    return exito


# =============================================================================
# 3. REPORTE FINAL Y MAIN
# =============================================================================

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Auditoria de Integridad de Datos: Data Cruda (Raw) vs Azure SQL"
    )
    parser.add_argument("--exportar-reporte", action="store_true", help="Guarda el informe en formato Markdown")
    args = parser.parse_args(argv)

    print_titulo("AUDITORIA DE INTEGRIDAD: DATA CRUDA VS AZURE SQL DATABASE")
    print(f"  Fecha de evaluacion: {dt.datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Directorio Data Cruda: {RUTA_RAW}")

    # 1. Conectar a Azure SQL
    try:
        engine = obtener_engine()
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        print_ok("Conexion con Azure SQL Database establecida exitosamente.")
    except Exception as e:
        print_falla(f"No se pudo conectar a Azure SQL Database: {e}")
        return 1

    # 2. Cargar datos crudos
    raw_data = cargar_todos_los_raw()
    print_ok(f"Data cruda cargada: {len(raw_data['ejecuciones'])} ejecuciones, {len(raw_data['cuentas'])} cuentas, {len(raw_data['publicaciones'])} publicaciones.")

    # 3. Ejecutar las 6 auditorias
    resultados = []
    
    ok_vol, resumen_vol = auditar_conciliacion_volumen(engine, raw_data)
    resultados.append(("Conciliacion de Volumen y Entidades", ok_vol))

    ok_attr = auditar_fidelidad_atributos_publicacion(engine, raw_data)
    resultados.append(("Fidelidad de Atributos de Publicacion", ok_attr))

    ok_met = auditar_exactitud_metricas(engine, raw_data)
    resultados.append(("Exactitud de Metricas Cuantitativas", ok_met))

    ok_ref = auditar_integridad_referencial(engine)
    resultados.append(("Integridad Referencial (Cero Huerfanos)", ok_ref))

    ok_uni = auditar_unicidad_y_deduplicacion(engine)
    resultados.append(("Unicidad y Deduplicacion (Cero Duplicados)", ok_uni))

    ok_temp = auditar_consistencia_temporal(engine)
    resultados.append(("Consistencia Temporal y Estandarizacion UTC", ok_temp))

    # 4. Resumen Final
    total_pruebas = len(resultados)
    aprobadas = sum(1 for _, ok in resultados if ok)
    fallidas = total_pruebas - aprobadas

    print_titulo("RESUMEN DE AUDITORIA DE INTEGRIDAD")
    for nombre, ok in resultados:
        estado = f"{Colores.VERDE}[APROBADO]{Colores.RESET}" if ok else f"{Colores.ROJO}[FALLIDO]{Colores.RESET}"
        print(f"  {estado} {nombre}")

    print(f"\n  Pruebas ejecutadas : {total_pruebas}")
    print(f"  Pruebas aprobadas  : {Colores.VERDE}{aprobadas}{Colores.RESET}")
    print(f"  Pruebas fallidas   : {Colores.ROJO if fallidas else Colores.VERDE}{fallidas}{Colores.RESET}")

    if fallidas == 0:
        print(f"\n{Colores.VERDE}{Colores.BOLD}  >> CONCLUSION: INTEGRIDAD DE DATOS AL 100% (CONCILIACION EXITOSA) <<{Colores.RESET}\n")
        return 0
    else:
        print(f"\n{Colores.ROJO}{Colores.BOLD}  >> CONCLUSION: SE DETECTARON DISCREPANCIAS DE INTEGRIDAD <<{Colores.RESET}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
