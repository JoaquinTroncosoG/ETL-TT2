#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Orquestador principal del pipeline ETL.
Punto de entrada unico para ejecutar los procesos de extraccion, transformacion y carga.

USO:
    python main.py
    python main.py --limite 5     # Ejecuta extracciones rapidas (solo 5 registros)
"""

import argparse
import sys

# Importar los modulos de extraccion
from src.extract import extract_youtube
from src.extract import extract_instagram

def main():
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline ETL completo.")
    parser.add_argument("--limite", type=int, help="Limite de registros a extraer (util para pruebas de concepto)")
    args, _ = parser.parse_known_args()

    print("=" * 60)
    print(" INICIANDO PIPELINE ETL")
    print("=" * 60)

    # Preparar los argumentos para pasarselos a los modulos internos
    argumentos_extraccion = []
    if args.limite:
        argumentos_extraccion.extend(["--limite", str(args.limite)])

    # ---------------------------------------------------------
    # FASE 1: EXTRACCION
    # ---------------------------------------------------------
    print("\n>>> FASE 1: EXTRACCION DE DATOS <<<")
    
    print("\n[1/2] Ejecutando extraccion de YouTube...")
    try:
        codigo_yt = extract_youtube.main(argumentos_extraccion)
    except Exception as e:
        print(f"Error critico llamando a modulo YouTube: {e}")
        codigo_yt = 1

    print("\n[2/2] Ejecutando extraccion de Instagram...")
    try:
        codigo_ig = extract_instagram.main(argumentos_extraccion)
    except Exception as e:
        print(f"Error critico llamando a modulo Instagram: {e}")
        codigo_ig = 1

    # ---------------------------------------------------------
    # RESUMEN Y SALIDA
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print(" RESUMEN DE EJECUCION")
    print("=" * 60)
    print(f"YouTube:   {'[EXITO]' if codigo_yt == 0 else '[FALLA]'}")
    print(f"Instagram: {'[EXITO]' if codigo_ig == 0 else '[FALLA]'}")
    
    if codigo_yt != 0 or codigo_ig != 0:
        print("\n[!] El pipeline termino con advertencias o errores. Revisa los logs arriba.")
        sys.exit(1)
    else:
        print("\n[OK] Pipeline ETL ejecutado correctamente.")
        sys.exit(0)

if __name__ == "__main__":
    main()
