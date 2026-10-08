"""Fixtures compartilhadas da suíte da ingestão (`plugins/ingestion`).

O Airflow põe `plugins/` no PYTHONPATH; aqui o pytest faz o mesmo, para os testes
importarem `ingestion` como a DAG importa, sem depender do Makefile.
"""

import sys
from pathlib import Path

PLUGINS_DIR = Path(__file__).resolve().parents[2] / "plugins"

if str(PLUGINS_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGINS_DIR))
