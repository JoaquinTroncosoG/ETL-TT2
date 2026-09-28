#!/usr/bin/env python3
"""Atajo: muestra el estado de todas las credenciales del pipeline."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.utils.auth import main
sys.argv = [sys.argv[0], "--estado"]
sys.exit(main())
