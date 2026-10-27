#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejecutor de DDL para Azure SQL Database
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)

Lee el script SQL 'sql/crear_tablas.sql' y ejecuta la creacion de las
Tablas de Hechos y Dimensiones en Azure SQL Database.

USO
---
    python src/load/crear_tablas.py
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

try:
    import pyodbc
except ImportError:
    sys.exit("Falta la libreria 'pyodbc'. Instala con: pip install pyodbc")

RAIZ = Path(__file__).resolve().parents[2]
load_dotenv(RAIZ / ".env")

SQL_PATH = RAIZ / "sql" / "crear_tablas.sql"

SERVER = os.getenv("AZURE_SERVER")
DATABASE = os.getenv("AZURE_DATABASE")
USER = os.getenv("AZURE_USER")
PASSWORD = os.getenv("AZURE_PASSWORD")


def obtener_driver():
    """Detecta el mejor driver ODBC disponible en el sistema operativo."""
    instalados = pyodbc.drivers()
    preferidos = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for d in preferidos:
        if d in instalados:
            return d
    return instalados[0] if instalados else None


def construir_cadena_conexion(driver):
    """Genera la cadena de conexion compatible con Azure SQL."""
    if "ODBC Driver 18" in driver or "ODBC Driver 17" in driver:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER=tcp:{SERVER},1433;"
            f"DATABASE={DATABASE};"
            f"UID={USER};"
            f"PWD={PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={SERVER},1433;"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
    )


def separar_sentencias_sql(contenido):
    """
    Divide el script SQL en bloques ejecutables, omitiendo comentarios y lineas vacias.
    """
    bloques = []
    bloque_actual = []
    
    for linea in contenido.splitlines():
        linea_limpia = linea.strip()
        if not linea_limpia or linea_limpia.startswith("--"):
            continue
        
        bloque_actual.append(linea)
        if linea_limpia.endswith(";"):
            bloques.append("\n".join(bloque_actual))
            bloque_actual = []
            
    if bloque_actual:
        bloques.append("\n".join(bloque_actual))
        
    return bloques


def ejecutar_ddl():
    if not all([SERVER, DATABASE, USER, PASSWORD]):
        sys.exit("[!] Error: Faltan credenciales de Azure SQL en el archivo .env")

    if not SQL_PATH.exists():
        sys.exit(f"[!] Error: No se encontro el archivo SQL en {SQL_PATH}")

    driver = obtener_driver()
    if not driver:
        sys.exit("[!] Error: No se encontraron drivers ODBC instalados.")

    print("=" * 65)
    print("  CREACION DE TABLAS DDL - MODELO DIMENSIONAL")
    print(f"  Servidor:  {SERVER}")
    print(f"  BD Destino:{DATABASE}")
    print(f"  Driver:    {driver}")
    print("=" * 65)

    conn_str = construir_cadena_conexion(driver)
    
    print("\n[1/3] Conectando a Azure SQL Database...")
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        conn.autocommit = True
        cursor = conn.cursor()
        print("      [OK] Conectado exitosamente.")
    except Exception as e:
        print(f"\n[FALLA] Error al conectar con Azure SQL: {e}")
        return False

    print("\n[2/3] Leyendo y ejecutando sentencias DDL...")
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    sentencias = separar_sentencias_sql(sql_text)
    print(f"      Total de bloques SQL detectados: {len(sentencias)}")

    ejecutadas = 0
    for idx, stmt in enumerate(sentencias, 1):
        try:
            cursor.execute(stmt)
            ejecutadas += 1
        except Exception as e:
            print(f"\n[!] Error en bloque {idx}: {e}")
            print(f"Sentencia:\n{stmt[:200]}...")
            conn.close()
            return False

    print(f"      [OK] Se ejecutaron las {ejecutadas} sentencias correctamente.")

    print("\n[3/3] Verificando tablas creadas en INFORMATION_SCHEMA...")
    cursor.execute("""
        SELECT TABLE_NAME, TABLE_TYPE 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo'
        ORDER BY TABLE_NAME
    """)
    tablas = cursor.fetchall()

    print("\n" + "=" * 65)
    print("  TABLAS EXISTENTES EN AZURE SQL (dbo)")
    print("=" * 65)
    esperadas = {
        "DIM_TIEMPO", "DIM_PLATAFORMA", "DIM_CUENTA", 
        "DIM_PUBLICACION", "DIM_EJECUCION", "HECHOS_METRICAS_PUBLICACION"
    }
    creadas = set()
    for t_name, t_type in tablas:
        creadas.add(t_name)
        marca = "[OK]" if t_name in esperadas else "    "
        print(f"  {marca} {t_name:<32} ({t_type})")

    faltantes = esperadas - creadas
    if faltantes:
        print(f"\n[ADVERTENCIA] Faltan las siguientes tablas: {faltantes}")
        exito = False
    else:
        print("\n[OK] Todas las tablas de hechos y dimensiones fueron verificadas!")
        exito = True

    conn.close()
    return exito


def main():
    exito = ejecutar_ddl()
    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
