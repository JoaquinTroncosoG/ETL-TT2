#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orquestador principal del pipeline ETL Multi-Plataforma.
Punto de entrada unico para ejecutar los procesos de extraccion, transformacion y carga.

USO:
    python main.py                     # Ejecuta el flujo completo (E -> T -> L)
    python main.py --limite 5          # Extracciones rapidas (solo 5 registros)
    python main.py --solo-extraer      # Ejecuta unicamente la Fase 1 (Extraccion)
    python main.py --solo-transformar  # Ejecuta unicamente la Fase 2 (Transformacion)
    python main.py --solo-cargar       # Ejecuta unicamente la Fase 3 (Carga a Azure)
    python main.py --plataforma youtube # Procesa unicamente una plataforma
"""

import argparse
import datetime as dt
import sys
import time

# Importar modulos de cada fase
from src.extract import extract_youtube
from src.extract import extract_instagram
from src.transform import transform_data
from src.load import cargar_datos


def parsear_argumentos():
    parser = argparse.ArgumentParser(
        description="Orquestador integral del Pipeline ETL (Extraccion, Transformacion y Carga)."
    )
    parser.add_argument("--limite", type=int, help="Limite de registros a extraer en las APIs")
    parser.add_argument("--plataforma", choices=["youtube", "instagram"], help="Filtrar por una sola plataforma")

    # Modos de ejecucion por fase
    grupo_fases = parser.add_mutually_exclusive_group()
    grupo_fases.add_argument("--solo-extraer", action="store_true", help="Ejecutar unicamente la Fase 1 (Extraccion)")
    grupo_fases.add_argument("--solo-transformar", action="store_true", help="Ejecutar unicamente la Fase 2 (Transformacion)")
    grupo_fases.add_argument("--solo-cargar", action="store_true", help="Ejecutar unicamente la Fase 3 (Carga a Azure SQL)")

    return parser.parse_args()


def ejecutar_extraccion(args) -> dict:
    print("\n" + "=" * 65)
    print(" >>> FASE 1: EXTRACCION DE DATOS DESDE APIS <<<")
    print("=" * 65)

    argumentos = []
    if args.limite:
        argumentos.extend(["--limite", str(args.limite)])

    estados = {}

    # YouTube
    if not args.plataforma or args.plataforma == "youtube":
        print("\n[1/2] Extrayendo datos de YouTube Data API v3...")
        try:
            codigo_yt = extract_youtube.main(argumentos)
            estados["youtube"] = codigo_yt == 0
        except Exception as e:
            print(f"Error en extraccion de YouTube: {e}")
            estados["youtube"] = False

    # Instagram
    if not args.plataforma or args.plataforma == "instagram":
        print("\n[2/2] Extrayendo datos de Instagram Graph API...")
        try:
            codigo_ig = extract_instagram.main(argumentos)
            estados["instagram"] = codigo_ig == 0
        except Exception as e:
            print(f"Error en extraccion de Instagram: {e}")
            estados["instagram"] = False

    return estados


def ejecutar_transformacion(args) -> bool:
    print("\n" + "=" * 65)
    print(" >>> FASE 2: TRANSFORMACION Y CALIDAD DE DATOS <<<")
    print("=" * 65)

    argumentos = ["--formatos", "csv", "xlsx"]
    if args.plataforma:
        argumentos.extend(["--solo", args.plataforma])

    try:
        codigo = transform_data.main(argumentos)
        return codigo == 0
    except Exception as e:
        print(f"Error en fase de transformacion: {e}")
        return False


def ejecutar_carga(args) -> bool:
    print("\n" + "=" * 65)
    print(" >>> FASE 3: CARGA AL DATA WAREHOUSE (AZURE SQL) <<<")
    print("=" * 65)

    argumentos = []
    if args.plataforma:
        argumentos.extend(["--solo", args.plataforma])

    try:
        codigo = cargar_datos.main(argumentos)
        return codigo == 0
    except Exception as e:
        print(f"Error en fase de carga: {e}")
        return False


def main():
    args = parsear_argumentos()
    inicio = time.time()

    print("=" * 65)
    print("  PIPELINE ETL CENTRALIZADO MULTI-PLATAFORMA")
    print(f"  Inicio: {dt.datetime.now():%d-%m-%Y %H:%M:%S}")
    print("=" * 65)

    ejecutar_todas = not (args.solo_extraer or args.solo_transformar or args.solo_cargar)

    resumen = {}

    # 1. Extraccion
    if ejecutar_todas or args.solo_extraer:
        resumen["Extraccion"] = ejecutar_extraccion(args)

    # 2. Transformacion
    if ejecutar_todas or args.solo_transformar:
        exito_t = ejecutar_transformacion(args)
        resumen["Transformacion"] = "[OK]" if exito_t else "[FALLA]"

    # 3. Carga
    if ejecutar_todas or args.solo_cargar:
        exito_l = ejecutar_carga(args)
        resumen["Carga Azure"] = "[OK]" if exito_l else "[FALLA]"

    duracion = time.time() - inicio

    # Resumen final
    print("\n" + "=" * 65)
    print("  RESUMEN FINAL DE EJECUCION DEL PIPELINE")
    print("=" * 65)
    for fase, estado in resumen.items():
        if isinstance(estado, dict):
            detalles = ", ".join(f"{plat}: {'[OK]' if ok else '[AVISO]'}" for plat, ok in estado.items())
            print(f"  {fase:<20} {detalles}")
        else:
            print(f"  {fase:<20} {estado}")

    print(f"\n  Duracion total: {duracion:.2f} segundos")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    sys.exit(main())
