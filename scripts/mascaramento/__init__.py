"""Mascaramento de PII no raw/ do lake. O ponto de entrada é scripts/mascarar_minio.py."""

import sys
from pathlib import Path

# plugins/ (ClienteMinio) está na PYTHONPATH dentro do container Airflow; rodando
# standalone, adiciona plugins/ ao sys.path para o import resolver.
_plugins = Path(__file__).resolve().parents[2] / "plugins"
if _plugins.is_dir() and str(_plugins) not in sys.path:
    sys.path.insert(0, str(_plugins))
