#!/usr/bin/env python3
"""Atajo: fuerza la re-autorizacion de YouTube (abre el navegador)."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.utils.auth import main
sys.argv = [sys.argv[0], "--renovar-youtube"]
sys.exit(main())
