"""Trava de sincronia entre o ambiente de testes e o da imagem do Airflow (ADR-0014).

- pyproject.toml, grupo `runtime`: o que a imagem precisa, com pin exato.
- infra/docker/airflow/requirements.lock.txt: o que a imagem instala, resolvido a partir do
  grupo `runtime` contra a imagem base por infra/docker/airflow/compile-lock.sh.
- uv.lock: o ambiente dos testes e da CI, restrito às versões da imagem.

Os três mudam juntos (`make lock`). Estes testes reprovam o PR que mexeu num sem os outros.
"""

import tomllib
from importlib import metadata
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())
UV_LOCK = tomllib.loads((REPO_ROOT / "uv.lock").read_text())
IMAGE_LOCK = REPO_ROOT / "infra" / "docker" / "airflow" / "requirements.lock.txt"


def _runtime() -> dict[str, str]:
    """Nome -> versão do pin exato do grupo `runtime`."""
    pins = {}
    for linha in PYPROJECT["dependency-groups"]["runtime"]:
        req = Requirement(linha)
        (spec,) = req.specifier
        assert spec.operator == "==", f"runtime sem pin exato: {linha}"
        pins[canonicalize_name(req.name)] = spec.version
    return pins


def _imagem() -> dict[str, str]:
    pins = {}
    for linha in IMAGE_LOCK.read_text().splitlines():
        if linha and not linha[0].isspace() and not linha.startswith("#"):
            req = Requirement(linha)
            pins[canonicalize_name(req.name)] = next(iter(req.specifier)).version
    return pins


def test_runtime_pina_a_versao_da_imagem():
    imagem = _imagem()
    divergentes = [
        f"{nome}: pyproject={versao} imagem={imagem.get(nome, 'ausente')}"
        for nome, versao in _runtime().items()
        if imagem.get(nome) != versao
    ]
    assert not divergentes, "Rode `make lock`:\n" + "\n".join(divergentes)


def test_uv_lock_usa_as_versoes_da_imagem():
    imagem = _imagem()
    divergentes = [
        f"{p['name']}: uv.lock={p['version']} imagem={imagem[canonicalize_name(p['name'])]}"
        for p in UV_LOCK["package"]
        if "version" in p and imagem.get(canonicalize_name(p["name"]), p["version"]) != p["version"]
    ]
    assert not divergentes, "Rode `make lock`:\n" + "\n".join(divergentes)


def test_ambiente_instalado_bate_com_o_runtime():
    """Pega o ambiente que não foi reinstalado depois de mudar o pyproject."""
    divergentes = []
    for nome, versao in _runtime().items():
        try:
            instalada = metadata.version(nome)
        except metadata.PackageNotFoundError:
            instalada = "ausente"
        if instalada != versao:
            divergentes.append(f"{nome}: instalado={instalada} pyproject={versao}")
    assert not divergentes, "Reinstale com `uv sync`:\n" + "\n".join(divergentes)
