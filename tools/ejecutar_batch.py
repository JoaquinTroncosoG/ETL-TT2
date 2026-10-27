#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wrapper para Ejecucion Batch Desatendida (CRON / Programador de Tareas)
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Este script esta disenado para ser invocado de forma no interactiva por:
    - Programador de Tareas de Windows (Task Scheduler / schtasks)
    - Demonio CRON de Linux (crontab)
    - Servicios o scripts de inicio del sistema operativo

Funcionalidades:
    1. Asegura que el directorio de ejecucion sea la raiz del proyecto.
    2. Redirige toda la salida a un archivo de log historico fechado en 'logs/'.
    3. Mantiene una copia del archivo 'logs/ultimo_batch.log' para facil monitoreo.
    4. Ejecuta el pipeline completo (Extraccion -> Transformacion -> Carga).
    5. Retorna el codigo de salida del sistema operativo (0 exito, 1 fallo).

USO
---
    python tools/ejecutar_batch.py
    python tools/ejecutar_batch.py --limite 10
    python tools/ejecutar_batch.py --plataforma youtube
"""

import datetime as dt
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Tuple

# Asegurar que el directorio de trabajo sea la raiz del proyecto
RAIZ = Path(__file__).resolve().parents[1]
os.chdir(RAIZ)
sys.path.insert(0, str(RAIZ))

from main import main as ejecutar_pipeline


def configurar_logging_batch() -> Tuple[Path, Path]:
    carpeta_logs = RAIZ / "logs"
    carpeta_logs.mkdir(parents=True, exist_ok=True)

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo_log_fecha = carpeta_logs / f"batch_{timestamp}.log"
    archivo_ultimo_log = carpeta_logs / "ultimo_batch.log"

    # Formateador para consola y archivo
    formato = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Handler archivo fechado
    fh = logging.FileHandler(archivo_log_fecha, encoding="utf-8")
    fh.setFormatter(formato)
    root_logger.addHandler(fh)

    # Handler consola
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formato)
    root_logger.addHandler(sh)

    return archivo_log_fecha, archivo_ultimo_log


def main():
    inicio = dt.datetime.now()
    log_fecha, log_ultimo = configurar_logging_batch()
    log = logging.getLogger("batch_scheduler")

    log.info("=" * 65)
    log.info("  INICIANDO EJECUCION BATCH DESATENDIDA")
    log.info("  Hora de inicio: %s", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Directorio raiz: %s", RAIZ)
    log.info("  Archivo de log:  %s", log_fecha)
    log.info("=" * 65)

    codigo_salida = 0
    try:
        # Ejecutar el pipeline principal con los argumentos recibidos
        codigo_salida = ejecutar_pipeline()
    except Exception as exc:
        log.error("Excepcion no controlada durante el proceso batch: %s", exc, exc_info=True)
        codigo_salida = 1
    finally:
        duracion = (dt.datetime.now() - inicio).total_seconds()
        estado = "EXITOSO" if codigo_salida == 0 else "FALLIDO"
        log.info("\n" + "=" * 65)
        log.info("  PROCESO BATCH FINALIZADO [%s]", estado)
        log.info("  Duracion total: %.2f segundos", duracion)
        log.info("  Codigo de salida: %d", codigo_salida)
        log.info("=" * 65)

        # Copiar el log como ultimo_batch.log para consulta rapida
        try:
            shutil.copy2(log_fecha, log_ultimo)
        except Exception:
            pass

    return codigo_salida


if __name__ == "__main__":
    sys.exit(main())
