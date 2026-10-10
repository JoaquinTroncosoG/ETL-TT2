#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Transformacion y limpieza de datos crudos
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Autor: Joaquin Troncoso G.

Fase 3 (Transformacion), segundo paso: toma los DataFrames planos que entrega
cargar_json.py y les aplica tres operaciones fundamentales de calidad de datos:

    1. TRATAMIENTO DE VALORES NULOS
       - Campos numericos (contadores, metricas): nulos se reemplazan por 0.
       - Campos de texto (caption, title, description): nulos se reemplazan
         por cadena vacia "".
       - Campos criticos (id, id_ejecucion): filas con nulo se eliminan
         porque carecen de identidad y no se pueden vincular a ninguna
         dimension en el Data Warehouse.

    2. ELIMINACION DE DUPLICADOS
       - Se detectan registros repetidos usando la combinacion de
         (id_ejecucion + id del recurso) como llave compuesta.
       - Se conserva la primera aparicion y se descartan las siguientes.
       - Se registra la cantidad de duplicados eliminados en el reporte.

    3. CASTEO DE TIPOS DE DATOS
       - Contadores que la API devuelve como string (ej. "1234") se
         convierten a int64.
       - Fechas ISO 8601 (ej. "2026-09-28T22:57:01+00:00") se parsean
         a datetime64[ns, UTC].
       - Duraciones ISO 8601 de YouTube (ej. "PT4M13S") se convierten
         a timedelta y luego a total de segundos (float).

Cada transformacion genera un log detallado por consola para trazabilidad
academica. Al final, guarda los DataFrames limpios en data/processed/ como
archivos CSV, listos para la fase de carga (Load).

USO
---
    python src/transform/transform_data.py
    python src/transform/transform_data.py --solo youtube
    python src/transform/transform_data.py --solo instagram
    python src/transform/transform_data.py --salida data/processed/prueba

Requiere haber ejecutado previamente los extractores para que existan
archivos JSON en data/raw/.
"""

import argparse
import datetime as dt
import logging
import re
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    sys.exit("Falta la libreria 'pandas'. Instala con: pip install -r requirements.txt")

# Importar el modulo de carga de JSON crudos (paso anterior del pipeline)
try:
    from src.transform.cargar_json import (
        cargar_todo_instagram,
        cargar_todo_youtube,
    )
except ImportError:
    # Soporte para ejecucion directa (sin -m)
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from src.transform.cargar_json import (
        cargar_todo_instagram,
        cargar_todo_youtube,
    )

# =========================================================================
# Configuracion general
# =========================================================================

RAIZ = Path(__file__).resolve().parents[2]
SALIDA_POR_DEFECTO = RAIZ / "data" / "processed"

log = logging.getLogger("transform_data")

# -------------------------------------------------------------------------
# Definicion de reglas de transformacion por tabla
# Cada entrada mapea el nombre de la columna a su tipo destino.
# Las columnas que no aparecen aqui se dejan tal cual vienen.
# -------------------------------------------------------------------------

# Valores especiales para indicar la accion sobre nulos
_ELIMINAR_FILA = "ELIMINAR_FILA"  # si el campo es nulo, se borra la fila entera
_TEXTO_VACIO = ""                 # nulos de texto se reemplazan por ""
_CERO = 0                         # nulos numericos se reemplazan por 0

REGLAS_INSTAGRAM = {
    "perfil": {
        "nulos": {
            "id": _ELIMINAR_FILA,
            "id_ejecucion": _ELIMINAR_FILA,
            "username": _TEXTO_VACIO,
            "name": _TEXTO_VACIO,
            "followers_count": _CERO,
            "follows_count": _CERO,
            "media_count": _CERO,
        },
        "duplicados": ["id_ejecucion", "id"],
        "tipos": {
            "followers_count": "int64",
            "follows_count": "int64",
            "media_count": "int64",
        },
    },
    "publicaciones": {
        "nulos": {
            "id": _ELIMINAR_FILA,
            "id_ejecucion": _ELIMINAR_FILA,
            "caption": _TEXTO_VACIO,
            "media_type": _TEXTO_VACIO,
            "media_product_type": _TEXTO_VACIO,
            "permalink": _TEXTO_VACIO,
            "like_count": _CERO,
            "comments_count": _CERO,
        },
        "duplicados": ["id_ejecucion", "id"],
        "tipos": {
            "like_count": "int64",
            "comments_count": "int64",
            "timestamp": "datetime",
        },
    },
    "metricas": {
        "nulos": {
            "id_ejecucion": _ELIMINAR_FILA,
            "id_publicacion": _ELIMINAR_FILA,
            "metrica": _TEXTO_VACIO,
            "periodo": _TEXTO_VACIO,
            "valor": _CERO,
        },
        "duplicados": ["id_ejecucion", "id_publicacion", "metrica"],
        "tipos": {
            "valor": "int64",
        },
    },
    "ejecucion": {
        "nulos": {
            "id_ejecucion": _ELIMINAR_FILA,
            "plataforma": _TEXTO_VACIO,
            "estado": _TEXTO_VACIO,
            "error_tipo": _TEXTO_VACIO,
            "error_codigo": _TEXTO_VACIO,
            "error_mensaje": _TEXTO_VACIO,
            "publicaciones_extraidas": _CERO,
            "llamadas_api": _CERO,
            "paginas_recorridas": _CERO,
            "duracion_segundos": _CERO,
        },
        "duplicados": ["id_ejecucion"],
        "tipos": {
            "fecha_hora_inicio_utc": "datetime",
            "fecha_hora_fin_utc": "datetime",
            "publicaciones_extraidas": "int64",
            "llamadas_api": "int64",
            "paginas_recorridas": "int64",
            "duracion_segundos": "float64",
        },
    },
}

REGLAS_YOUTUBE = {
    "canal": {
        "nulos": {
            "id": _ELIMINAR_FILA,
            "id_ejecucion": _ELIMINAR_FILA,
            "snippet.title": _TEXTO_VACIO,
            "snippet.customUrl": _TEXTO_VACIO,
            "statistics.subscriberCount": _CERO,
            "statistics.videoCount": _CERO,
            "statistics.viewCount": _CERO,
        },
        "duplicados": ["id_ejecucion", "id"],
        "tipos": {
            "statistics.subscriberCount": "int64",
            "statistics.videoCount": "int64",
            "statistics.viewCount": "int64",
            "snippet.publishedAt": "datetime",
        },
    },
    "videos": {
        "nulos": {
            "contentDetails.videoId": _ELIMINAR_FILA,
            "id_ejecucion": _ELIMINAR_FILA,
            "snippet.title": _TEXTO_VACIO,
            "snippet.description": _TEXTO_VACIO,
            "status.privacyStatus": _TEXTO_VACIO,
        },
        "duplicados": ["id_ejecucion", "contentDetails.videoId"],
        "tipos": {
            "contentDetails.videoPublishedAt": "datetime",
        },
    },
    "estadisticas": {
        "nulos": {
            "id": _ELIMINAR_FILA,
            "id_ejecucion": _ELIMINAR_FILA,
            "statistics.viewCount": _CERO,
            "statistics.likeCount": _CERO,
            "statistics.commentCount": _CERO,
            "contentDetails.duration": _TEXTO_VACIO,
        },
        "duplicados": ["id_ejecucion", "id"],
        "tipos": {
            "statistics.viewCount": "int64",
            "statistics.likeCount": "int64",
            "statistics.commentCount": "int64",
            "contentDetails.duration": "duracion_iso",
        },
    },
    "ejecucion": {
        "nulos": {
            "id_ejecucion": _ELIMINAR_FILA,
            "plataforma": _TEXTO_VACIO,
            "estado": _TEXTO_VACIO,
            "error_tipo": _TEXTO_VACIO,
            "error_codigo": _TEXTO_VACIO,
            "error_mensaje": _TEXTO_VACIO,
            "videos_extraidos": _CERO,
            "llamadas_api": _CERO,
            "paginas_recorridas": _CERO,
            "cuota_estimada": _CERO,
            "duracion_segundos": _CERO,
        },
        "duplicados": ["id_ejecucion"],
        "tipos": {
            "fecha_hora_inicio_utc": "datetime",
            "fecha_hora_fin_utc": "datetime",
            "videos_extraidos": "int64",
            "llamadas_api": "int64",
            "paginas_recorridas": "int64",
            "cuota_estimada": "int64",
            "duracion_segundos": "float64",
        },
    },
}


# =========================================================================
# Funciones de transformacion
# =========================================================================

def _parsear_duracion_iso(valor):
    """
    Convierte una duracion ISO 8601 de YouTube (ej. 'PT4M13S', 'PT1H2M3S')
    a segundos totales (float). Retorna None si el formato no es reconocido.

    Ejemplos:
        'PT4M13S'   -> 253.0
        'PT1H2M3S'  -> 3723.0
        'PT30S'     -> 30.0
    """
    if not isinstance(valor, str) or not valor.startswith("PT"):
        return None
    patron = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")
    match = patron.match(valor)
    if not match:
        return None
    horas = int(match.group(1) or 0)
    minutos = int(match.group(2) or 0)
    segundos = int(match.group(3) or 0)
    return float(horas * 3600 + minutos * 60 + segundos)


def tratar_nulos(df, reglas_nulos, nombre_tabla):
    """
    Aplica el tratamiento de valores nulos segun las reglas definidas.
    Retorna el DataFrame limpio y la cantidad de filas eliminadas.
    """
    filas_originales = len(df)
    filas_eliminadas = 0

    for columna, accion in reglas_nulos.items():
        if columna not in df.columns:
            continue

        nulos_en_columna = df[columna].isna().sum()

        if accion == _ELIMINAR_FILA:
            antes = len(df)
            df = df.dropna(subset=[columna])
            eliminadas = antes - len(df)
            filas_eliminadas += eliminadas
            if eliminadas > 0:
                log.info("    [NULOS] %s.%s: %d fila(s) eliminada(s) (campo critico nulo)",
                         nombre_tabla, columna, eliminadas)
        elif nulos_en_columna > 0:
            df[columna] = df[columna].fillna(accion)
            log.info("    [NULOS] %s.%s: %d nulo(s) reemplazado(s) por %r",
                     nombre_tabla, columna, nulos_en_columna, accion)

    if filas_eliminadas == 0 and all(
        df[c].isna().sum() == 0 for c in reglas_nulos if c in df.columns
    ):
        log.info("    [NULOS] %s: sin valores nulos detectados", nombre_tabla)

    return df, filas_eliminadas


def eliminar_duplicados(df, columnas_clave, nombre_tabla):
    """
    Elimina filas duplicadas basandose en las columnas clave.
    Conserva la primera aparicion de cada combinacion.
    Retorna el DataFrame sin duplicados y la cantidad eliminada.
    """
    columnas_presentes = [c for c in columnas_clave if c in df.columns]
    if not columnas_presentes:
        return df, 0

    antes = len(df)
    df = df.drop_duplicates(subset=columnas_presentes, keep="first")
    eliminados = antes - len(df)

    if eliminados > 0:
        log.info("    [DUPLICADOS] %s: %d fila(s) duplicada(s) eliminada(s) (clave: %s)",
                 nombre_tabla, eliminados, " + ".join(columnas_presentes))
    else:
        log.info("    [DUPLICADOS] %s: sin duplicados detectados", nombre_tabla)

    return df, eliminados


def castear_tipos(df, reglas_tipos, nombre_tabla):
    """
    Convierte los tipos de datos de las columnas segun las reglas.
    Tipos soportados: int64, float64, datetime, duracion_iso.
    Retorna el DataFrame con los tipos corregidos y un contador de casteos.
    """
    casteos_realizados = 0

    for columna, tipo_destino in reglas_tipos.items():
        if columna not in df.columns:
            continue

        tipo_original = str(df[columna].dtype)

        try:
            if tipo_destino == "int64":
                df[columna] = pd.to_numeric(df[columna], errors="coerce").fillna(0).astype("int64")
                casteos_realizados += 1

            elif tipo_destino == "float64":
                df[columna] = pd.to_numeric(df[columna], errors="coerce").fillna(0.0)
                casteos_realizados += 1

            elif tipo_destino == "datetime":
                df[columna] = pd.to_datetime(df[columna], errors="coerce", utc=True)
                casteos_realizados += 1

            elif tipo_destino == "duracion_iso":
                df[columna + "_segundos"] = df[columna].apply(_parsear_duracion_iso)
                df[columna + "_segundos"] = df[columna + "_segundos"].fillna(0.0)
                casteos_realizados += 1

            tipo_nuevo = str(df[columna].dtype)
            log.info("    [CASTEO] %s.%s: %s -> %s",
                     nombre_tabla, columna, tipo_original, tipo_nuevo)

        except Exception as exc:
            log.warning("    [CASTEO] %s.%s: fallo al convertir a %s (%s)",
                        nombre_tabla, columna, tipo_destino, exc)

    if casteos_realizados == 0:
        log.info("    [CASTEO] %s: sin conversiones necesarias", nombre_tabla)

    return df, casteos_realizados


# =========================================================================
# Orquestacion: aplicar las 3 transformaciones a un conjunto de tablas
# =========================================================================

def transformar_tablas(tablas, reglas, plataforma):
    """
    Recibe un dict {nombre: DataFrame} y le aplica las 3 transformaciones
    en orden. Devuelve el dict con los DataFrames limpios y un resumen.
    """
    resumen = {}
    limpias = {}

    log.info("=" * 55)
    log.info("  TRANSFORMANDO: %s", plataforma.upper())
    log.info("=" * 55)

    for nombre, df in tablas.items():
        if nombre not in reglas:
            limpias[nombre] = df
            continue

        regla = reglas[nombre]
        filas_antes = len(df)
        log.info("\n  --- Tabla: %s (%d filas, %d columnas) ---",
                 nombre, filas_antes, len(df.columns))

        # Paso 1: Nulos
        df, nulos_eliminados = tratar_nulos(df.copy(), regla.get("nulos", {}), nombre)

        # Paso 2: Duplicados
        df, duplicados_eliminados = eliminar_duplicados(df, regla.get("duplicados", []), nombre)

        # Paso 3: Casteo
        df, casteos = castear_tipos(df, regla.get("tipos", {}), nombre)

        limpias[nombre] = df.reset_index(drop=True)
        resumen[nombre] = {
            "filas_originales": filas_antes,
            "filas_finales": len(df),
            "nulos_eliminados": nulos_eliminados,
            "duplicados_eliminados": duplicados_eliminados,
            "casteos_realizados": casteos,
        }

    return limpias, resumen


# =========================================================================
# Guardado de resultados (CSV)
# =========================================================================

def guardar_csv(tablas, carpeta, plataforma):
    """Guarda cada DataFrame como CSV en la carpeta de destino."""
    destino = Path(carpeta) / plataforma
    destino.mkdir(parents=True, exist_ok=True)

    archivos = []
    for nombre, df in tablas.items():
        ruta = destino / f"{nombre}.csv"
        df.to_csv(ruta, index=False, encoding="utf-8")
        archivos.append(ruta)
        log.info("  [GUARDADO] %s -> %s (%d filas)", nombre, ruta, len(df))

    return archivos


# =========================================================================
# Reporte final
# =========================================================================

def imprimir_resumen(resumen_ig, resumen_yt):
    """Imprime una tabla resumen de las transformaciones realizadas."""
    print("\n" + "=" * 65)
    print("  RESUMEN DE TRANSFORMACIONES")
    print("=" * 65)

    todos = []
    if resumen_ig:
        todos.append(("Instagram", resumen_ig))
    if resumen_yt:
        todos.append(("YouTube", resumen_yt))

    for plataforma, resumen in todos:
        print(f"\n  [{plataforma}]")
        print(f"  {'Tabla':<20} {'Antes':>6} {'Despues':>8} {'Nulos':>6} {'Dupl.':>6} {'Cast.':>6}")
        print("  " + "-" * 52)
        for nombre, stats in resumen.items():
            print(f"  {nombre:<20} {stats['filas_originales']:>6} "
                  f"{stats['filas_finales']:>8} {stats['nulos_eliminados']:>6} "
                  f"{stats['duplicados_eliminados']:>6} {stats['casteos_realizados']:>6}")


# =========================================================================
# Punto de entrada (CLI)
# =========================================================================

def configurar_logging():
    manejador = logging.StreamHandler(sys.stdout)
    manejador.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[manejador])


def parsear_argumentos():
    parser = argparse.ArgumentParser(
        description="Transforma y limpia los datos crudos extraidos de las APIs. "
                    "Trata nulos, elimina duplicados y castea tipos de datos.")
    parser.add_argument("--solo", choices=["instagram", "youtube"],
                        help="Procesar unicamente una plataforma")
    parser.add_argument("--salida", default=str(SALIDA_POR_DEFECTO),
                        help=f"Carpeta de salida (por defecto: {SALIDA_POR_DEFECTO})")
    return parser.parse_args()


def main():
    args = parsear_argumentos()
    configurar_logging()

    print("\n" + "=" * 55)
    print("  PIPELINE ETL - Fase de Transformacion")
    print(f"  Ejecutado: {dt.datetime.now():%d-%m-%Y %H:%M}")
    print("=" * 55)

    resumen_ig = None
    resumen_yt = None

    # -- Instagram --
    if args.solo is None or args.solo == "instagram":
        log.info("\nCargando datos crudos de Instagram...")
        tablas_ig = cargar_todo_instagram()
        limpias_ig, resumen_ig = transformar_tablas(tablas_ig, REGLAS_INSTAGRAM, "instagram")
        guardar_csv(limpias_ig, args.salida, "instagram")

    # -- YouTube --
    if args.solo is None or args.solo == "youtube":
        log.info("\nCargando datos crudos de YouTube...")
        tablas_yt = cargar_todo_youtube()
        limpias_yt, resumen_yt = transformar_tablas(tablas_yt, REGLAS_YOUTUBE, "youtube")
        guardar_csv(limpias_yt, args.salida, "youtube")

    # -- Resumen --
    imprimir_resumen(resumen_ig, resumen_yt)

    print(f"\nArchivos limpios guardados en: {args.salida}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
