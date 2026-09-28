#!/usr/bin/env python3
"""Atajo: canjea un token corto de Instagram por uno de larga duracion (60 dias)."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.utils.auth import main

if len(sys.argv) < 2:
    print("Error: Debes proporcionar el token corto de Meta.")
    print("Uso: python canjear_instagram.py <TOKEN_CORTO>")
    sys.exit(1)

token_corto = sys.argv[1]
sys.argv = [sys.argv[0], "--canjear-instagram", token_corto]
sys.exit(main())
