"""Contrato de todo Extractor.

Toda estratégia grava o dado da fonte em disco, no formato original, e devolve um
RawFile por parte, sem carregar o dataset em memória.

Cada implementação registrada entra em IMPLEMENTATIONS e herda estes testes.
Vazio até a Fase 2: o pytest reporta como skipped.
"""

import pytest

IMPLEMENTATIONS: list[str] = []


@pytest.mark.parametrize("name", IMPLEMENTATIONS)
def test_contract(name: str) -> None:
    raise NotImplementedError(name)
