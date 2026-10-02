"""Trava de sincronia entre o ambiente de testes e o da imagem do Airflow.

As dependencias moram em dois arquivos:

- requirements.txt: o que a imagem do Airflow instala, ou seja, producao.
- pyproject.toml: o ambiente do Poetry, onde rodam os testes e o CI.

O Dependabot abre um PR por arquivo. Com eles aceitos separadamente, e com
metade do pyproject em "*", o CI testava dbt, cosmos e numpy em versoes que a
imagem nao roda. Estes testes reprovam essa divergencia.

A regra: toda dependencia de runtime do pyproject aparece no requirements.txt
com o mesmo pin exato. As unicas excecoes sao as de SO_FORA_DA_IMAGEM.
"""

import tomllib
from importlib import metadata
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS_PATH = REPO_ROOT / "requirements.txt"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

# Usadas so em scripts/governance/, que roda fora do Airflow.
SO_FORA_DA_IMAGEM = {"great-expectations", "sqlglot"}


def _dependencias_do_pyproject() -> dict[str, str]:
    """Dependencias de runtime do Poetry, sem o grupo dev."""
    dados = tomllib.loads(PYPROJECT_PATH.read_text())
    deps = dados["tool"]["poetry"]["dependencies"]
    return {
        canonicalize_name(nome): spec["version"] if isinstance(spec, dict) else spec
        for nome, spec in deps.items()
        if nome != "python" and canonicalize_name(nome) not in SO_FORA_DA_IMAGEM
    }


def _pins_do_requirements() -> dict[str, str | None]:
    """Nome -> versao do pin exato, ou None quando o requisito e uma faixa."""
    pins: dict[str, str | None] = {}
    for linha in REQUIREMENTS_PATH.read_text().splitlines():
        if not linha.strip() or linha.lstrip().startswith("#"):
            continue
        req = Requirement(linha)
        exatos = [s.version for s in req.specifier if s.operator == "=="]
        pins[canonicalize_name(req.name)] = exatos[0] if len(exatos) == 1 else None
    return pins


def test_pyproject_pina_a_mesma_versao_do_requirements():
    pins = _pins_do_requirements()
    divergentes = [
        f"{nome}: pyproject={spec!r} requirements={pins.get(nome, 'ausente')!r}"
        for nome, spec in _dependencias_do_pyproject().items()
        if spec != pins.get(nome)
    ]
    assert not divergentes, (
        "pyproject.toml diverge da imagem do Airflow. Use o mesmo pin exato nos "
        "dois arquivos, no mesmo PR:\n" + "\n".join(divergentes)
    )


def test_ambiente_instalado_bate_com_o_requirements():
    """Pega o ambiente que nao foi reinstalado depois de mudar o pyproject."""
    pins = _pins_do_requirements()
    divergentes = []
    for nome in _dependencias_do_pyproject():
        try:
            instalada = metadata.version(nome)
        except metadata.PackageNotFoundError:
            instalada = "ausente"
        if instalada != pins.get(nome):
            divergentes.append(
                f"{nome}: instalado={instalada} requirements={pins.get(nome)}"
            )
    assert not divergentes, (
        "O ambiente instalado nao e o da imagem. Reinstale com `poetry install`:\n"
        + "\n".join(divergentes)
    )
