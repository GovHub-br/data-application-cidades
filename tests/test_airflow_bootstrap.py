"""Guardas do bootstrap do Airflow local (infra/airflow/bootstrap/).

O `airflow-init` do compose importa esses JSONs a cada `make up`. As DAGs leem as
Variables com `default=`, então uma Variable que falta não quebra nada: a
bacen_sgs_ingest_dag termina verde sem ingerir nenhuma série e a ibge_ingest_dag
gera zero tasks. Por isso o example precisa declarar tudo o que o código lê.

Estes testes leem só o que é versionado (`*.example.json` e `pools.json`).
`variables.json` e `connections.json` têm credenciais reais e ficam fora do git.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterator

import pytest

RAIZ = Path(__file__).resolve().parents[1]
BOOTSTRAP = RAIZ / "infra" / "airflow" / "bootstrap"
CODIGO = [RAIZ / "dags", RAIZ / "plugins", RAIZ / "helpers"]

VARIABLE_GET = re.compile(r"Variable\.get\(\s*[\"']([^\"']+)[\"']")
# `conn_id="x"` literal (ex.: `postgres_conn_id="postgres_default"`), ou o padrão
# `"x_default"` de um parâmetro de função.
CONNECTION = re.compile(r"conn_id\s*=\s*[\"']([^\"']+)[\"']|[\"'](\w+_default)[\"']")
# `pool="x"`, `{"pool": "x"}` ou uma constante `POOL_X = "x"` passada adiante.
POOL = re.compile(r"(?:\bpool\s*=|[\"']pool[\"']\s*:|\bPOOL_\w+\s*=)\s*[\"']([^\"']+)[\"']")

# Campos aceitos pelo `airflow connections import` (argumentos de Connection).
CAMPOS_CONNECTION = {"conn_type", "description", "host", "port", "schema"}
CAMPOS_CONNECTION |= {"login", "password", "extra"}
# Folha do example cujo caminho tem um destes nomes guarda credencial e precisa ser
# placeholder `<...>`, para ninguém colar um valor real no arquivo versionado.
SENSIVEL = re.compile(r"password|senha|api_key|email|login", re.IGNORECASE)
PLACEHOLDER = re.compile(r"^<[^<>]+>$")


def _carregar(nome: str) -> dict[str, Any]:
    dados = json.loads((BOOTSTRAP / nome).read_text(encoding="utf-8"))
    assert isinstance(dados, dict), f"{nome}: esperado um objeto nome -> definição"
    return dados


def _referencias(padrao: re.Pattern[str]) -> dict[str, set[str]]:
    """Nome referenciado no código -> arquivos onde aparece."""
    achados: dict[str, set[str]] = {}
    for pasta in CODIGO:
        for arquivo in pasta.rglob("*.py"):
            for grupos in padrao.findall(arquivo.read_text(encoding="utf-8")):
                for nome in grupos if isinstance(grupos, tuple) else (grupos,):
                    if nome:
                        achados.setdefault(nome, set()).add(str(arquivo.relative_to(RAIZ)))
    return achados


def _folhas(valor: Any, caminho: str = "") -> Iterator[tuple[str, Any]]:
    if isinstance(valor, dict):
        for chave, filho in valor.items():
            yield from _folhas(filho, f"{caminho}.{chave}" if caminho else chave)
    elif isinstance(valor, list):
        for indice, filho in enumerate(valor):
            yield from _folhas(filho, f"{caminho}[{indice}]")
    else:
        yield caminho, valor


def _faltando(declarados: dict[str, Any], padrao: re.Pattern[str]) -> list[str]:
    return [
        f"{nome} (em {', '.join(sorted(arquivos))})"
        for nome, arquivos in sorted(_referencias(padrao).items())
        if nome not in declarados
    ]


def test_toda_variable_lida_pelo_codigo_esta_no_example() -> None:
    faltando = _faltando(_carregar("variables.example.json"), VARIABLE_GET)
    assert not faltando, f"declare em variables.example.json: {faltando}"


def test_toda_connection_usada_pelo_codigo_esta_no_example() -> None:
    faltando = _faltando(_carregar("connections.example.json"), CONNECTION)
    assert not faltando, f"declare em connections.example.json: {faltando}"


def test_todo_pool_usado_pelo_codigo_esta_no_pools_json() -> None:
    faltando = _faltando(_carregar("pools.json"), POOL)
    assert not faltando, f"declare em pools.json: {faltando}"


def test_variables_no_formato_com_descricao() -> None:
    """`{chave: {description, value}}`: o import grava a descrição junto."""
    for chave, definicao in _carregar("variables.example.json").items():
        assert isinstance(definicao, dict), chave
        assert set(definicao) == {"description", "value"}, chave
        assert definicao["description"].strip(), f"{chave}: descrição vazia"


def test_connections_so_com_campos_que_o_import_aceita() -> None:
    for conn_id, definicao in _carregar("connections.example.json").items():
        assert definicao.get("conn_type"), f"{conn_id}: falta conn_type"
        assert set(definicao) <= CAMPOS_CONNECTION, f"{conn_id}: campo desconhecido"
        assert isinstance(definicao.get("port", 0), int), f"{conn_id}: porta é inteiro"


def test_pools_com_slots_e_descricao() -> None:
    """Sem `slots` e `description` o `airflow pools import` recusa o pool."""
    for nome, definicao in _carregar("pools.json").items():
        assert isinstance(definicao.get("slots"), int) and definicao["slots"] >= 1, nome
        assert isinstance(definicao.get("description"), str), nome


@pytest.mark.parametrize("nome", ["variables.example.json", "connections.example.json"])
def test_example_nao_carrega_credencial_real(nome: str) -> None:
    for chave, definicao in _carregar(nome).items():
        valor = definicao.get("value", definicao)
        for caminho, folha in _folhas(valor, chave):
            if SENSIVEL.search(caminho) and isinstance(folha, str):
                assert PLACEHOLDER.match(folha), f"{nome}: {caminho} deve ser <placeholder>"


def test_arquivos_com_credencial_ficam_fora_do_git() -> None:
    ignorados = (RAIZ / ".gitignore").read_text(encoding="utf-8").splitlines()
    for nome in ("variables.json", "connections.json"):
        assert f"infra/airflow/bootstrap/{nome}" in ignorados, nome
