"""
common.py: caminhos e utilidades compartilhadas pelo pipeline de documentacao.
"""

import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT_DIR / "docs-pages"
DBT_DIR = ROOT_DIR / "airflow_lappis" / "dags" / "dbt"
DAGS_DIR = ROOT_DIR / "airflow_lappis" / "dags"
PLUGINS_DIR = ROOT_DIR / "airflow_lappis" / "plugins"
SRC_DIR = DOCS_DIR / "src"
DATA_DIR = SRC_DIR / "_data"
ACERVO_DIR = SRC_DIR / "acervo"
TEMPLATES_DIR = SRC_DIR / "templates"
ASSETS_DIR = SRC_DIR / "assets"
SITE_DIR = DOCS_DIR / "site"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger("docs")


def write_json(nome: str, payload: dict[str, Any]) -> Path:
    """Grava um JSON de acervo em docs-pages/src/_data/<nome>.json."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    destino = DATA_DIR / f"{nome}.json"
    destino.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return destino


def read_json(nome: str) -> dict[str, Any]:
    """Le um JSON de acervo. Devolve dict vazio se ainda nao existir."""
    origem = DATA_DIR / f"{nome}.json"
    if not origem.exists():
        return {}
    dados: dict[str, Any] = json.loads(origem.read_text(encoding="utf-8"))
    return dados


def run(cmd: list[str], cwd: Path | None = None) -> str:
    """Executa um comando e devolve o stdout. Levanta erro se o comando falhar."""
    resultado = subprocess.run(
        cmd,
        cwd=cwd or ROOT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return resultado.stdout
