#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stress test y tiempos de ejecucion del pipeline (Fase 5)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Pasa volumenes crecientes de datos sinteticos (tools/generar_sinteticos.py)
por las mismas funciones que usa la carga real y mide cada etapa:

    lectura          cargar_todo_youtube / cargar_todo_instagram (JSON -> DataFrames)
    normalizacion    normalizar_youtube / normalizar_instagram
    transformacion   lectura + normalizacion
    carga            cargar_plataforma hacia Azure SQL Database

Cada volumen es el total de publicaciones, repartido en partes iguales entre
YouTube e Instagram. La transformacion corre en local y se repite siempre
REPETICIONES veces. La carga gasta computo del plan gratuito de Azure, por lo
que se limita segun REPETICIONES_CARGA. La memoria pico se mide con
tracemalloc en una pasada aparte para no alterar los tiempos.

Los datos sinteticos llevan el prefijo "STRESS-" y se borran de la base
despues de cada repeticion.

USO
---
    python tools/stress_test.py                    # plan completo
    python tools/stress_test.py --sin-carga        # solo transformacion (no usa Azure)
    python tools/stress_test.py --volumenes 100    # prueba rapida
    python tools/stress_test.py --solo-limpiar     # borra restos STRESS- de la base
    python tools/stress_test.py --solo-graficos    # regenera los PNG desde el CSV
"""

import argparse
import logging
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

import pandas as pd
from sqlalchemy import text

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from src.load.cargar_datos import cargar_plataforma, obtener_engine
from src.transform.cargar_json import cargar_todo_instagram, cargar_todo_youtube
from src.transform.normalizar import normalizar_instagram, normalizar_youtube
from tools.generar_sinteticos import PREFIJO, generar_instagram, generar_youtube

VOLUMENES = [100, 1_000, 10_000, 100_000]
REPETICIONES = 3
REPETICIONES_CARGA = {100: 3, 1_000: 3, 10_000: 1}

CARPETA_RESULTADOS = RAIZ / "resultados_stress"
CSV_DETALLE = CARPETA_RESULTADOS / "resultados_stress_detalle.csv"
CSV_RESUMEN = CARPETA_RESULTADOS / "resultados_stress.csv"

ETAPAS = ["lectura", "normalizacion", "transformacion", "carga"]

log = logging.getLogger("stress_test")


def generar(n, carpeta):
    n_yt = n // 2
    generar_youtube(n_yt, carpeta / "youtube", semilla=n)
    generar_instagram(n - n_yt, carpeta / "instagram", semilla=n)


def transformar(carpeta):
    t0 = time.perf_counter()
    crudo_yt = cargar_todo_youtube(carpeta / "youtube")
    crudo_ig = cargar_todo_instagram(carpeta / "instagram")
    t1 = time.perf_counter()
    norm = {"youtube": normalizar_youtube(crudo_yt), "instagram": normalizar_instagram(crudo_ig)}
    t2 = time.perf_counter()
    return norm, t1 - t0, t2 - t1


def memoria_pico_mb(carpeta):
    tracemalloc.start()
    transformar(carpeta)
    _, pico = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return pico / 1024 ** 2


def ids_tiempo(conn):
    return {r[0] for r in conn.execute(text("SELECT id_tiempo FROM dbo.DIM_TIEMPO"))}


def limpiar(engine, tiempos_previos=None):
    """Borra de la base todo lo marcado con PREFIJO, respetando el orden de las FK."""
    patron = {"p": f"{PREFIJO}%"}
    sentencias = [
        "DELETE FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_ejecucion LIKE :p",
        "DELETE FROM dbo.DIM_PUBLICACION WHERE id_publicacion_externo LIKE :p",
        "DELETE FROM dbo.DIM_CUENTA WHERE id_cuenta_externo LIKE :p",
        "DELETE FROM dbo.DIM_EJECUCION WHERE id_ejecucion LIKE :p",
    ]
    borrados = {}
    with engine.begin() as conn:
        for sql in sentencias:
            tabla = sql.split()[2].split(".")[1]
            borrados[tabla] = conn.execute(text(sql), patron).rowcount

        if tiempos_previos is not None:
            nuevos = ids_tiempo(conn) - tiempos_previos
            if nuevos:
                lista = ",".join(str(i) for i in nuevos)
                borrados["DIM_TIEMPO"] = conn.execute(text(f"""
                    DELETE t FROM dbo.DIM_TIEMPO t
                    WHERE t.id_tiempo IN ({lista})
                      AND NOT EXISTS (SELECT 1 FROM dbo.HECHOS_METRICAS_PUBLICACION h WHERE h.id_tiempo = t.id_tiempo)
                      AND NOT EXISTS (SELECT 1 FROM dbo.DIM_PUBLICACION p WHERE CAST(p.fecha_hora_publicacion_utc AS DATE) = t.fecha)
                      AND NOT EXISTS (SELECT 1 FROM dbo.DIM_EJECUCION e WHERE CAST(e.fecha_hora_inicio_utc AS DATE) = t.fecha)
                """)).rowcount
    return borrados


def restos_sinteticos(engine):
    patron = {"p": f"{PREFIJO}%"}
    with engine.connect() as conn:
        return sum(conn.execute(text(sql), patron).scalar() for sql in [
            "SELECT COUNT(*) FROM dbo.HECHOS_METRICAS_PUBLICACION WHERE id_ejecucion LIKE :p",
            "SELECT COUNT(*) FROM dbo.DIM_PUBLICACION WHERE id_publicacion_externo LIKE :p",
            "SELECT COUNT(*) FROM dbo.DIM_CUENTA WHERE id_cuenta_externo LIKE :p",
            "SELECT COUNT(*) FROM dbo.DIM_EJECUCION WHERE id_ejecucion LIKE :p",
        ])


def ejecutar(volumenes, con_carga):
    engine = obtener_engine() if con_carga else None
    tiempos_previos = None
    if engine is not None:
        limpiar(engine)
        with engine.connect() as conn:
            tiempos_previos = ids_tiempo(conn)

    filas = []
    try:
        for n in volumenes:
            reps_carga = REPETICIONES_CARGA.get(n, 0) if con_carga else 0
            for rep in range(1, REPETICIONES + 1):
                with tempfile.TemporaryDirectory(prefix="stress_") as tmp:
                    carpeta = Path(tmp)
                    generar(n, carpeta)
                    norm, t_lec, t_norm = transformar(carpeta)
                    fila = {"volumen": n, "repeticion": rep, "lectura": t_lec,
                            "normalizacion": t_norm, "transformacion": t_lec + t_norm,
                            "carga": None, "limpieza": None, "memoria_pico_mb": None}

                    if rep <= reps_carga:
                        t0 = time.perf_counter()
                        for plataforma, tablas in norm.items():
                            cargar_plataforma(engine, tablas, plataforma)
                        fila["carga"] = time.perf_counter() - t0
                        t0 = time.perf_counter()
                        limpiar(engine, tiempos_previos)
                        fila["limpieza"] = time.perf_counter() - t0
                        if restos_sinteticos(engine):
                            raise RuntimeError("La limpieza dejo registros STRESS- en la base")

                    if rep == REPETICIONES:
                        fila["memoria_pico_mb"] = memoria_pico_mb(carpeta)

                fila["total"] = fila["transformacion"] + (fila["carga"] or 0)
                filas.append(fila)
                log.info("n=%-7d rep=%d  transformacion=%.2fs  carga=%s", n, rep, fila["transformacion"],
                         f"{fila['carga']:.2f}s" if fila["carga"] is not None else "-")
    finally:
        if engine is not None:
            limpiar(engine, tiempos_previos)

    return pd.DataFrame(filas)


def resumir(detalle):
    resumen = detalle.groupby("volumen").agg(
        repeticiones=("repeticion", "count"),
        repeticiones_carga=("carga", "count"),
        **{f"{e}_s": (e, "mean") for e in ETAPAS},
        limpieza_s=("limpieza", "mean"),
        memoria_pico_mb=("memoria_pico_mb", "max"),
    ).reset_index()
    resumen["total_s"] = resumen["transformacion_s"] + resumen["carga_s"]
    for e in ["transformacion", "carga", "total"]:
        resumen[f"{e}_reg_por_s"] = resumen["volumen"] / resumen[f"{e}_s"]
    return resumen.round(4)


def graficar(resumen):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    for etapa in ["lectura", "normalizacion", "transformacion", "carga"]:
        datos = resumen.dropna(subset=[f"{etapa}_s"])
        ax.plot(datos["volumen"], datos[f"{etapa}_s"], marker="o", label=etapa)
    ax.set(xscale="log", yscale="log", xlabel="Publicaciones (escala log)",
           ylabel="Tiempo promedio [s] (escala log)", title="Tiempo vs volumen por etapa")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(CARPETA_RESULTADOS / "tiempo_vs_volumen.png", dpi=150)

    fig, ax = plt.subplots(figsize=(8, 5))
    for etapa in ["transformacion", "carga"]:
        datos = resumen.dropna(subset=[f"{etapa}_reg_por_s"])
        ax.plot(datos["volumen"], datos[f"{etapa}_reg_por_s"], marker="o", label=etapa)
    ax.set(xscale="log", xlabel="Publicaciones (escala log)", ylabel="Registros por segundo",
           title="Rendimiento por etapa")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(CARPETA_RESULTADOS / "registros_por_segundo.png", dpi=150)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(resumen["volumen"], resumen["memoria_pico_mb"], marker="o")
    ax.set(xscale="log", xlabel="Publicaciones (escala log)", ylabel="Memoria pico [MB]",
           title="Memoria pico de la transformacion (tracemalloc)")
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()
    fig.savefig(CARPETA_RESULTADOS / "memoria_vs_volumen.png", dpi=150)
    plt.close("all")


def parsear_argumentos(argumentos=None):
    parser = argparse.ArgumentParser(description="Stress test del pipeline ETL con datos sinteticos.")
    parser.add_argument("--volumenes", type=int, nargs="+", default=VOLUMENES)
    parser.add_argument("--sin-carga", action="store_true", help="No conectarse a Azure SQL")
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--solo-limpiar", action="store_true", help="Borrar restos STRESS- y salir")
    grupo.add_argument("--solo-graficos", action="store_true", help="Regenerar PNG desde el CSV")
    return parser.parse_args(argumentos)


def main(argumentos=None):
    args = parsear_argumentos(argumentos)
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    log.setLevel(logging.INFO)
    CARPETA_RESULTADOS.mkdir(exist_ok=True)

    if args.solo_limpiar:
        log.info("Registros borrados: %s", limpiar(obtener_engine()))
        return 0

    if args.solo_graficos:
        graficar(pd.read_csv(CSV_RESUMEN))
        return 0

    detalle = ejecutar(args.volumenes, con_carga=not args.sin_carga)
    resumen = resumir(detalle)
    detalle.round(4).to_csv(CSV_DETALLE, index=False)
    resumen.to_csv(CSV_RESUMEN, index=False)
    graficar(resumen)
    print(resumen.to_string(index=False))
    log.info("Resultados en %s", CARPETA_RESULTADOS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
