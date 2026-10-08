"""Contrato de todo FileConverter.

A partir de um arquivo de texto, todo formato gera Parquet só com colunas string, sem
tipar nem renomear.

Cada implementação registrada entra em IMPLEMENTATIONS e herda estes testes.
Vazio até a Fase 3: o pytest reporta como skipped.
"""

import pytest

IMPLEMENTATIONS: list[str] = []


@pytest.mark.parametrize("name", IMPLEMENTATIONS)
def test_contract(name: str) -> None:
    raise NotImplementedError(name)
