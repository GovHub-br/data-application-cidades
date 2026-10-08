"""Contrato dos passos do pipeline.

Extract_to_raw e convert_to_staging recebem e devolvem só caminhos.

Cada implementação registrada entra em IMPLEMENTATIONS e herda estes testes.
Vazio até a Fase 5: o pytest reporta como skipped.
"""

import pytest

IMPLEMENTATIONS: list[str] = []


@pytest.mark.parametrize("name", IMPLEMENTATIONS)
def test_contract(name: str) -> None:
    raise NotImplementedError(name)
