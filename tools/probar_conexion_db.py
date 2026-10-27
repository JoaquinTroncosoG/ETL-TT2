#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Verificador de conexion a Azure SQL Database.
Pipeline Centralizado de Extraccion y Limpieza de Datos Multi-Plataforma (ETL)
"""

import os
import socket
import sys
from pathlib import Path
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ / ".env")

try:
    import pyodbc
except ImportError:
    sys.exit("Falta la libreria 'pyodbc'. Instala con: pip install pyodbc")

SERVER = os.getenv("AZURE_SERVER")
DATABASE = os.getenv("AZURE_DATABASE")
USER = os.getenv("AZURE_USER")
PASSWORD = os.getenv("AZURE_PASSWORD")


def test_socket(host, port=1433, timeout=5):
    print(f"\n[1/3] Verificando resolucion DNS y puerto {port}...")
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        print(f"      [OK] Puerto {port} accesible en {host}")
        return True
    except Exception as e:
        print(f"      [FALLA] No se pudo conectar al puerto {port}: {e}")
        return False


def listar_drivers():
    print("\n[2/3] Verificando drivers ODBC instalados en el sistema...")
    drivers = pyodbc.drivers()
    print("      Drivers encontrados:", drivers)
    
    preferidos = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]
    for d in preferidos:
        if d in drivers:
            print(f"      [OK] Driver seleccionado para prueba: '{d}'")
            return d
    return drivers[0] if drivers else None


def test_conexion(driver):
    print(f"\n[3/3] Probando autenticacion y conexion con driver '{driver}'...")
    
    if "ODBC Driver 18" in driver or "ODBC Driver 17" in driver:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER=tcp:{SERVER},1433;"
            f"DATABASE={DATABASE};"
            f"UID={USER};"
            f"PWD={PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={SERVER},1433;"
            f"DATABASE={DATABASE};"
            f"UID={USER};"
            f"PWD={PASSWORD};"
        )
    
    try:
        with pyodbc.connect(conn_str, timeout=15) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION, DB_NAME(), SUSER_SNAME()")
            version, db, user = cursor.fetchone()
            print("\n" + "=" * 60)
            print("  CONEXION EXITOSA A AZURE SQL DATABASE!")
            print("=" * 60)
            print(f"Base de datos activa: {db}")
            print(f"Usuario autenticado:  {user}")
            print(f"Version del motor:    {version.splitlines()[0]}")
            return True
    except pyodbc.Error as e:
        print("\n" + "=" * 60)
        print("  ERROR DE CONEXION")
        print("=" * 60)
        msg = str(e)
        print(f"Detalle: {msg}")
        
        if "Cannot open server" in msg or "is not allowed to access the server" in msg:
            print("\n>>> DIAGNOSTICO DE FIREWALL <<<")
            print("El servidor Azure SQL rechazo la IP. Debe agregarse a las reglas de firewall.")
        elif "Azure Active Directory only authentication is enabled" in msg:
            print("\n>>> DIAGNOSTICO DE AUTENTICACION <<<")
            print("Azure SQL tiene activado 'Solo autenticacion de Microsoft Entra'.")
        elif "Login failed for user" in msg or "18456" in msg:
            print("\n>>> DIAGNOSTICO DE CREDENCIALES <<<")
            print("El usuario o la contrasena son incorrectos, o no se ha establecido la contrasena")
            print("del administrador de SQL en Azure Portal (opcion 'Restablecer contrasena').")
        return False


def main():
    if not all([SERVER, DATABASE, USER, PASSWORD]):
        print("[!] Faltan variables de Azure en el archivo .env")
        sys.exit(1)
        
    print(f"Servidor: {SERVER}")
    print(f"Base de datos: {DATABASE}")
    print(f"Usuario: {USER}")
    
    if not test_socket(SERVER):
        sys.exit(1)
        
    driver = listar_drivers()
    if not driver:
        print("[!] No hay drivers ODBC instalados.")
        sys.exit(1)
        
    exito = test_conexion(driver)
    sys.exit(0 if exito else 1)


if __name__ == "__main__":
    main()
